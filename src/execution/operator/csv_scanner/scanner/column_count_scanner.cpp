#include "duckdb/execution/operator/csv_scanner/scanner/column_count_scanner.hpp"

namespace duckdb {

ColumnCountResult::ColumnCountResult(CSVStates &states, CSVStateMachine &state_machine)
    : ScannerResult(states, state_machine) {
}

void ColumnCountResult::AddValue(ColumnCountResult &result, const idx_t buffer_pos) {
	result.current_column_count++;
}

inline void ColumnCountResult::InternalAddRow() {
	column_counts[result_position++] = current_column_count + 1;
	current_column_count = 0;
}

idx_t &ColumnCountResult::operator[](size_t index) {
	return column_counts[index];
}

bool ColumnCountResult::AddRow(ColumnCountResult &result, const idx_t buffer_pos) {
	result.InternalAddRow();
	if (!result.states.EmptyLastValue()) {
		result.last_value_always_empty = false;
	}
	if (result.result_position >= STANDARD_VECTOR_SIZE) {
		// We sniffed enough rows
		return true;
	}
	return false;
}

void ColumnCountResult::InvalidState(ColumnCountResult &result) {
	result.result_position = 0;
	result.error = true;
}

bool ColumnCountResult::EmptyLine(ColumnCountResult &result, const idx_t buffer_pos) {
	// nop
	return false;
}

ColumnCountScanner::ColumnCountScanner(shared_ptr<CSVBufferManager> buffer_manager,
                                       shared_ptr<CSVStateMachine> state_machine,
                                       shared_ptr<CSVErrorHandler> error_handler)
    : BaseScanner(buffer_manager, state_machine, error_handler), result(states, *state_machine), column_count(1) {
	sniffing = true;
}

unique_ptr<StringValueScanner> ColumnCountScanner::UpgradeToStringValueScanner() {
	auto scanner = make_uniq<StringValueScanner>(buffer_manager, state_machine, error_handler);
	scanner->sniffing = true;
	return scanner;
}

ColumnCountResult *ColumnCountScanner::ParseChunk() {
	result.result_position = 0;
	column_count = 1;
	ParseChunkInternal();
	return &result;
}

ColumnCountResult *ColumnCountScanner::GetResult() {
	return &result;
}

void ColumnCountScanner::Initialize() {
	states.Initialize(CSVState::RECORD_SEPARATOR);
}

void ColumnCountScanner::Process() {
	// Run on this buffer
	for (; iterator.pos.buffer_pos < cur_buffer_handle->actual_size; iterator.pos.buffer_pos++) {
		if (ProcessCharacter(*this, buffer_handle_ptr[iterator.pos.buffer_pos], iterator.pos.buffer_pos, result)) {
			return;
		}
	}
}

void ColumnCountScanner::FinalizeChunkProcess() {
	if (result.result_position == STANDARD_VECTOR_SIZE || result.error) {
		// We are done
		return;
	}
	// We run until we have a full chunk, or we are done scanning
	while (!FinishedFile() && result.result_position < STANDARD_VECTOR_SIZE && !result.error) {
		if (iterator.pos.buffer_pos == cur_buffer_handle->actual_size) {
			// Move to next buffer
			cur_buffer_handle = buffer_manager->GetBuffer(++iterator.pos.buffer_idx);
			if (!cur_buffer_handle) {
				buffer_handle_ptr = nullptr;
				if (states.EmptyLine() || states.NewRow() || states.IsCurrentNewRow()) {
					return;
				}
				if (states.IsCurrentDelimiter()) {
					result.current_column_count++;
				}
				// This means we reached the end of the file, we must add a last line if there is any to be added
				result.InternalAddRow();
				return;
			}
			iterator.pos.buffer_pos = 0;
			buffer_handle_ptr = cur_buffer_handle->Ptr();
		}
		Process();
	}
}
} // namespace duckdb
