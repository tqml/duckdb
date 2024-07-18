#include "duckdb/execution/index/art/leaf.hpp"

#include "duckdb/common/types.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/iterator.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/execution/index/art/prefix.hpp"

namespace duckdb {

void Leaf::New(Node &node, const row_t row_id) {
	D_ASSERT(row_id < MAX_ROW_ID_LOCAL);

	node.Clear();
	node.SetMetadata(static_cast<uint8_t>(NType::LEAF_INLINED));
	node.SetRowId(row_id);
}

void Leaf::New(ART &art, reference<Node> &node, const vector<ARTKey> &row_ids, const idx_t start, const idx_t count) {
	D_ASSERT(count > 1);
	D_ASSERT(!node.get().HasMetadata());

	ARTKeySection section(start, start + count - 1, 0, 0);
	art.Construct(row_ids, row_ids, node, section);
	node.get().SetGate();
}

void MergeInlined(ART &art, Node &l_node, Node &r_node) {
	D_ASSERT(r_node.GetType() == NType::LEAF_INLINED);

	// Create an ARTKey from the row ID.
	ArenaAllocator arena_allocator(Allocator::Get(art.db));
	auto logical_type = LogicalType(LogicalType::ROW_TYPE);
	auto key = ARTKey::CreateARTKey<row_t>(arena_allocator, logical_type, r_node.GetRowId());

	// Insert the key.
	art.Insert(l_node, key, 0, key);
	r_node.Clear();
}

void Leaf::Merge(ART &art, Node &l_node, Node &r_node) {
	D_ASSERT(l_node.HasMetadata());
	D_ASSERT(r_node.HasMetadata());

	// Copy the inlined row ID of r_node into l_node.
	if (r_node.GetType() == NType::LEAF_INLINED) {
		return MergeInlined(art, l_node, r_node);
	}

	// l_node has an inlined row ID, swap and insert.
	if (l_node.GetType() == NType::LEAF_INLINED) {
		auto temp_node = l_node;
		l_node = r_node;
		MergeInlined(art, l_node, temp_node);
		return r_node.Clear();
	}

	D_ASSERT(l_node.GetType() != NType::LEAF_INLINED && l_node.IsGate());
	D_ASSERT(r_node.GetType() != NType::LEAF_INLINED && r_node.IsGate());
	l_node.Merge(art, r_node);
}

void Leaf::InsertIntoInlined(ART &art, Node &node, reference<const ARTKey> row_id) {
	D_ASSERT(node.GetType() == NType::LEAF_INLINED);

	auto inlined_row_id = node.GetRowId();
	node.Clear();

	ArenaAllocator allocator(Allocator::Get(art.db));
	auto logical_type = LogicalType(LogicalType::ROW_TYPE);
	auto inlined_row_id_key = ARTKey::CreateARTKey<row_t>(allocator, logical_type, inlined_row_id);

	// Insert both row IDs into the nested ART.
	// Row IDs are always unique.
	art.Insert(node, inlined_row_id_key, 0, inlined_row_id_key);
	art.Insert(node, row_id, 0, row_id);
	node.SetGate();
}

bool Leaf::Remove(ART &art, reference<Node> &node, const ARTKey &row_id) {
	D_ASSERT(node.get().HasMetadata());

	if (node.get().GetType() == NType::LEAF_INLINED) {
		return node.get().GetRowId() == row_id.GetRowID();
	}
	if (!node.get().IsGate()) {
		// This is a deprecated leaf. We transform it.
		TransformToNested(art, node);
	}

	// Erase the row ID.
	art.Erase(node, row_id, 0, row_id);

	// Traverse the prefix.
	reference<const Node> prefix_node(node);
	while (prefix_node.get().GetType() == NType::PREFIX) {
		auto &prefix = Node::Ref<const Prefix>(art, prefix_node, NType::PREFIX);
		prefix_node = prefix.ptr;
		D_ASSERT(prefix.ptr.HasMetadata());
	}

	// The first non-prefix node is an inlined leaf.
	// Thus, we can inline it into the node directly.
	if (prefix_node.get().GetType() == NType::LEAF_INLINED) {
		auto remaining_row_id = prefix_node.get().GetRowId();
		Node::Free(art, node);
		New(node, remaining_row_id);
	}
	return false;
}

void Leaf::TransformToNested(ART &art, Node &node) {
	D_ASSERT(node.GetType() == NType::LEAF);

	ArenaAllocator allocator(Allocator::Get(art.db));
	Node root = Node();

	// Move all row IDs into the nested leaf.
	auto logical_type = LogicalType(LogicalType::ROW_TYPE);
	reference<const Node> leaf_ref(node);
	while (leaf_ref.get().HasMetadata()) {
		auto &leaf = Node::Ref<const Leaf>(art, leaf_ref, NType::LEAF);
		for (idx_t i = 0; i < leaf.count; i++) {
			auto row_id = ARTKey::CreateARTKey<row_t>(allocator, logical_type, leaf.row_ids[i]);
			art.Insert(root, row_id, 0, row_id);
		}
		leaf_ref = leaf.ptr;
	}

	root.SetGate();
	Node::Free(art, node);
	node = root;
}

void Leaf::TransformToDeprecated(ART &art, Node &node) {
	D_ASSERT(node.IsGate() || node.GetType() == NType::LEAF);

	// Early-out, if we never transformed this leaf.
	if (!node.IsGate()) {
		return;
	}

	// Collect all row IDs and free the nested leaf.
	vector<row_t> row_ids;
	Iterator it(art);
	it.FindMinimum(node);
	ARTKey empty_key = ARTKey();
	it.Scan(empty_key, NumericLimits<row_t>().Maximum(), row_ids, false);
	Node::Free(art, node);
	D_ASSERT(row_ids.size() > 1);

	// Create the deprecated leaf.
	idx_t remaining_count = row_ids.size();
	idx_t copy_count = 0;
	reference<Node> ref_node(node);
	while (remaining_count) {
		ref_node.get() = Node::GetAllocator(art, NType::LEAF).New();
		ref_node.get().SetMetadata(static_cast<uint8_t>(NType::LEAF));

		auto &leaf = Node::RefMutable<Leaf>(art, ref_node, NType::LEAF);
		leaf.count = UnsafeNumericCast<uint8_t>(MinValue((idx_t)Node::LEAF_SIZE, remaining_count));

		for (idx_t i = 0; i < leaf.count; i++) {
			leaf.row_ids[i] = row_ids[copy_count + i];
		}

		copy_count += leaf.count;
		remaining_count -= leaf.count;

		ref_node = leaf.ptr;
		leaf.ptr.Clear();
	}
}

//===--------------------------------------------------------------------===//
// Debug-only functions.
//===--------------------------------------------------------------------===//

bool Leaf::ContainsRowId(ART &art, const Node &node, const ARTKey &row_id) {
	D_ASSERT(node.HasMetadata());

	if (node.GetType() == NType::LEAF_INLINED) {
		return node.GetRowId() == row_id.GetRowID();
	}

	// Note: This is a DEBUG function. We only call this after ART::Insert, ART::Delete,
	// and ART::ConstructFromSorted. Thus, it can never have deprecated storage.
	D_ASSERT(node.IsGate());
	return art.Lookup(node, row_id, 0) != nullptr;
}

//===--------------------------------------------------------------------===//
// Deprecated code paths.
//===--------------------------------------------------------------------===//

void Leaf::DeprecatedFree(ART &art, Node &node) {
	D_ASSERT(node.GetType() == NType::LEAF);

	Node next_node;
	while (node.HasMetadata()) {
		next_node = Node::RefMutable<Leaf>(art, node, NType::LEAF).ptr;
		Node::GetAllocator(art, NType::LEAF).Free(node);
		node = next_node;
	}
	node.Clear();
}

bool Leaf::DeprecatedGetRowIds(ART &art, const Node &node, vector<row_t> &row_ids, idx_t max_count) {
	D_ASSERT(node.GetType() == NType::LEAF);

	// Push back all row IDs of this leaf.
	reference<const Node> last_leaf_ref(node);
	while (last_leaf_ref.get().HasMetadata()) {
		auto &leaf = Node::Ref<const Leaf>(art, last_leaf_ref, NType::LEAF);

		// Never return more than max_count row IDs.
		if (row_ids.size() + leaf.count > max_count) {
			return false;
		}
		for (idx_t i = 0; i < leaf.count; i++) {
			row_ids.push_back(leaf.row_ids[i]);
		}
		last_leaf_ref = leaf.ptr;
	}
	return true;
}

void Leaf::DeprecatedVacuum(ART &art, Node &node) {
	D_ASSERT(node.HasMetadata());
	D_ASSERT(node.GetType() == NType::LEAF);

	auto &allocator = Node::GetAllocator(art, NType::LEAF);
	reference<Node> node_ref(node);
	while (node_ref.get().HasMetadata()) {
		if (allocator.NeedsVacuum(node_ref)) {
			node_ref.get() = allocator.VacuumPointer(node_ref);
			node_ref.get().SetMetadata(static_cast<uint8_t>(NType::LEAF));
		}
		auto &leaf = Node::RefMutable<Leaf>(art, node_ref, NType::LEAF);
		node_ref = leaf.ptr;
	}
}

string Leaf::DeprecatedVerifyAndToString(ART &art, const Node &node, const bool only_verify) {
	D_ASSERT(node.GetType() == NType::LEAF);

	string str = "";
	reference<const Node> node_ref(node);

	while (node_ref.get().HasMetadata()) {
		auto &leaf = Node::Ref<const Leaf>(art, node_ref, NType::LEAF);
		D_ASSERT(leaf.count <= Node::LEAF_SIZE);

		str += "Leaf [count: " + to_string(leaf.count) + ", row IDs: ";
		for (idx_t i = 0; i < leaf.count; i++) {
			str += to_string(leaf.row_ids[i]) + "-";
		}
		str += "] ";
		node_ref = leaf.ptr;
	}

	return only_verify ? "" : str;
}

} // namespace duckdb
