"""
DuckDB validity mask
"""
struct ValidityMask
    all_valid::Bool
    writable::Bool
    data::Vector{UInt64}
end

ValidityMask(data::Vector{UInt64}) = ValidityMask(true, true, data)

const BITS_PER_VALUE = 64;

function get_entry_index(row_idx)
    return ((row_idx - 1) ÷ BITS_PER_VALUE) + 1
end

function get_index_in_entry(row_idx)
    return (row_idx - 1) % BITS_PER_VALUE
end

function setinvalid(mask::ValidityMask, index)
    if !mask.writable
        throw(InvalidInputException("Validity mask is not writable"))
    end
    entry_idx = get_entry_index(index)
    index_in_entry = get_index_in_entry(index)
    mask.data[entry_idx] &= ~(1 << index_in_entry)
    return
end

function isvalid(mask::ValidityMask, index)::Bool
    if mask.all_valid
        return true
    end
    entry_idx = get_entry_index(index)
    index_in_entry = get_index_in_entry(index)
    return (mask.data[entry_idx] & (1 << index_in_entry)) != 0
end

_all_valid(data::Vector) = all(==(typemax(eltype(data))), data)
all_valid(mask::ValidityMask) = !mask.writable ? mask.all_valid : _all_valid(mask.data)
