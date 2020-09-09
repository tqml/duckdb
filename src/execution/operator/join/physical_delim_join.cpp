#include "duckdb/execution/operator/join/physical_delim_join.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/operator/scan/physical_chunk_scan.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parallel/task_context.hpp"

using namespace std;

namespace duckdb {

class PhysicalDelimJoinState : public PhysicalOperatorState {
public:
	PhysicalDelimJoinState(PhysicalOperator &op, PhysicalOperator *left) : PhysicalOperatorState(op, left) {
	}

	unique_ptr<PhysicalOperatorState> join_state;
};

PhysicalDelimJoin::PhysicalDelimJoin(vector<LogicalType> types, unique_ptr<PhysicalOperator> original_join,
                                     vector<PhysicalOperator *> delim_scans)
    : PhysicalSink(PhysicalOperatorType::DELIM_JOIN, move(types)), join(move(original_join)), delim_scans(move(delim_scans)) {
	assert(join->children.size() == 2);
	// now for the original join
	// we take its left child, this is the side that we will duplicate eliminate
	children.push_back(move(join->children[0]));

	// we replace it with a PhysicalChunkCollectionScan, that scans the ChunkCollection that we keep cached
	// the actual chunk collection to scan will be created in the DelimJoinGlobalState
	auto cached_chunk_scan = make_unique<PhysicalChunkScan>(children[0]->GetTypes(), PhysicalOperatorType::CHUNK_SCAN);
	join->children[0] = move(cached_chunk_scan);
}

class DelimJoinGlobalState : public GlobalOperatorState {
public:
	DelimJoinGlobalState(PhysicalDelimJoin *delim_join)  {
		assert(delim_join->delim_scans.size() > 0);
		// for any duplicate eliminated scans in the RHS, point them to the duplicate eliminated chunk that we create here
		for (auto op : delim_join->delim_scans) {
			assert(op->type == PhysicalOperatorType::DELIM_SCAN);
			auto scan = (PhysicalChunkScan *)op;
			scan->collection = &delim_data;
		}
		// set up the delim join chunk to scan in the original join
		auto &cached_chunk_scan = (PhysicalChunkScan&) *delim_join->join->children[0];
		cached_chunk_scan.collection = &lhs_data;
	}

	ChunkCollection lhs_data;
	ChunkCollection delim_data;
	unique_ptr<GlobalOperatorState> distinct_state;
};


unique_ptr<GlobalOperatorState> PhysicalDelimJoin::GetGlobalState(ClientContext &context) {
	auto state = make_unique<DelimJoinGlobalState>(this);
	state->distinct_state = distinct->GetGlobalState(context);
	return move(state);
}

unique_ptr<LocalSinkState> PhysicalDelimJoin::GetLocalSinkState(ExecutionContext &context) {
	return distinct->GetLocalSinkState(context);
}

void PhysicalDelimJoin::Sink(ExecutionContext &context, GlobalOperatorState &state_, LocalSinkState &lstate,
                             DataChunk &input) {
	auto &state = (DelimJoinGlobalState &) state_;
	state.lhs_data.Append(input);
	distinct->Sink(context, *state.distinct_state, lstate, input);
}

void PhysicalDelimJoin::Finalize(ClientContext &client, unique_ptr<GlobalOperatorState> state) {
	auto &dstate = (DelimJoinGlobalState &) *state;
	// finalize the distinct HT
	distinct->Finalize(client, move(dstate.distinct_state));
	// materialize the distinct collection
	DataChunk delim_chunk;
	distinct->InitializeChunk(delim_chunk);
	auto distinct_state = distinct->GetOperatorState();
	ThreadContext thread(client);
	TaskContext task;
	ExecutionContext context(client, thread, task);
	while (true) {
		distinct->GetChunk(context, delim_chunk, distinct_state.get());
		if (delim_chunk.size() == 0) {
			break;
		}
		dstate.delim_data.Append(delim_chunk);
	}
	PhysicalSink::Finalize(client, move(state));
}

void PhysicalDelimJoin::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalDelimJoinState *>(state_);
	if (!state->join_state) {
		// create the state of the underlying join
		state->join_state = join->GetOperatorState();
	}
	// now pull from the RHS from the underlying join
	join->GetChunk(context, chunk, state->join_state.get());
}

unique_ptr<PhysicalOperatorState> PhysicalDelimJoin::GetOperatorState() {
	return make_unique<PhysicalDelimJoinState>(*this, children[0].get());
}

string PhysicalDelimJoin::ExtraRenderInformation() const {
	return join->ExtraRenderInformation();
}

} // namespace duckdb
