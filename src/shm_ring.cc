#include "trans/details/shm_ring.hpp"

#include <cstring>

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include "elog/elog.h"

namespace rf
{
namespace trans
{

ShmRing::ShmRing(std::string name, size_t slot_size, size_t slot_count)
    : name_(std::move(name)),
      slot_size_(slot_size),
      slot_count_(slot_count)
{
}

ShmRing::~ShmRing()
{
    if (base_) {
        munmap(base_, region_size_);
    }
    if (fd_ >= 0) {
        close(fd_);
    }
    if (is_writer_) {
        shm_unlink(name_.c_str());
    }
}

size_t ShmRing::slotStride() const
{
    return sizeof(SlotHeader) + slot_size_;
}

ShmRing::RingHeader* ShmRing::ringHeader() const
{
    return reinterpret_cast<RingHeader*>(base_);
}

ShmRing::SlotHeader* ShmRing::slotHeader(uint32_t index) const
{
    return reinterpret_cast<SlotHeader*>(
        static_cast<char*>(base_) + sizeof(RingHeader) + static_cast<size_t>(index) * slotStride());
}

char* ShmRing::slotPayload(uint32_t index) const
{
    return reinterpret_cast<char*>(slotHeader(index)) + sizeof(SlotHeader);
}

bool ShmRing::createAsWriter()
{
    // Best-effort cleanup of a segment left behind by a crashed previous run
    // under the same name.
    shm_unlink(name_.c_str());

    region_size_ = sizeof(RingHeader) + slot_count_ * slotStride();

    fd_ = shm_open(name_.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
    if (fd_ < 0) {
        elog::error("ShmRing::createAsWriter() shm_open failed for [{}]: {}", name_, strerror(errno));
        return false;
    }

    if (ftruncate(fd_, static_cast<off_t>(region_size_)) != 0) {
        elog::error("ShmRing::createAsWriter() ftruncate failed for [{}]: {}", name_, strerror(errno));
        close(fd_);
        fd_ = -1;
        shm_unlink(name_.c_str());
        return false;
    }

    base_ = mmap(nullptr, region_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
    if (base_ == MAP_FAILED) {
        elog::error("ShmRing::createAsWriter() mmap failed for [{}]: {}", name_, strerror(errno));
        base_ = nullptr;
        close(fd_);
        fd_ = -1;
        shm_unlink(name_.c_str());
        return false;
    }

    RingHeader* header = ringHeader();
    header->magic = kMagic;
    header->slot_count = static_cast<uint32_t>(slot_count_);
    header->slot_size = static_cast<uint32_t>(slot_size_);
    header->reserved = 0;

    for (uint32_t i = 0; i < slot_count_; ++i) {
        SlotHeader* slot = slotHeader(i);
        slot->seq.store(0, std::memory_order_relaxed);
        slot->size = 0;
        slot->reserved = 0;
    }

    is_writer_ = true;
    return true;
}

bool ShmRing::openAsReader()
{
    fd_ = shm_open(name_.c_str(), O_RDONLY, 0);
    if (fd_ < 0) {
        elog::error("ShmRing::openAsReader() shm_open failed for [{}]: {}", name_, strerror(errno));
        return false;
    }

    void* header_map = mmap(nullptr, sizeof(RingHeader), PROT_READ, MAP_SHARED, fd_, 0);
    if (header_map == MAP_FAILED) {
        elog::error("ShmRing::openAsReader() header mmap failed for [{}]: {}", name_, strerror(errno));
        close(fd_);
        fd_ = -1;
        return false;
    }

    RingHeader header{};
    std::memcpy(&header, header_map, sizeof(RingHeader));
    munmap(header_map, sizeof(RingHeader));

    if (header.magic != kMagic) {
        elog::error("ShmRing::openAsReader() bad magic for [{}]", name_);
        close(fd_);
        fd_ = -1;
        return false;
    }

    slot_count_ = header.slot_count;
    slot_size_ = header.slot_size;
    region_size_ = sizeof(RingHeader) + slot_count_ * slotStride();

    base_ = mmap(nullptr, region_size_, PROT_READ, MAP_SHARED, fd_, 0);
    if (base_ == MAP_FAILED) {
        elog::error("ShmRing::openAsReader() mmap failed for [{}]: {}", name_, strerror(errno));
        base_ = nullptr;
        close(fd_);
        fd_ = -1;
        return false;
    }

    is_writer_ = false;
    return true;
}

ShmRing::ReservedSlot ShmRing::reserveWrite(size_t size)
{
    if (size > slot_size_) {
        elog::error("ShmRing::reserveWrite() size {} exceeds slot size {} for [{}]", size, slot_size_, name_);
        return {};
    }

    const uint64_t cursor = write_cursor_.fetch_add(1, std::memory_order_relaxed);
    const uint32_t index = static_cast<uint32_t>(cursor % slot_count_);
    SlotHeader* slot = slotHeader(index);

    const uint64_t begin_seq = 2 * cursor + 1;
    slot->seq.store(begin_seq, std::memory_order_seq_cst);

    return {index, begin_seq, slotPayload(index)};
}

ShmRing::SlotHandle ShmRing::commitWrite(const ReservedSlot& reserved, size_t size)
{
    SlotHeader* slot = slotHeader(reserved.slot_index);
    slot->size = static_cast<uint32_t>(size);

    const uint64_t end_seq = reserved.begin_seq + 1;
    slot->seq.store(end_seq, std::memory_order_seq_cst);

    return {reserved.slot_index, end_seq, static_cast<uint32_t>(size)};
}

ShmRing::SlotHandle ShmRing::write(const void* data, size_t size)
{
    const ReservedSlot reserved = reserveWrite(size);
    if (!reserved.payload) {
        return {};
    }

    std::memcpy(reserved.payload, data, size);
    return commitWrite(reserved, size);
}

ShmRing::PeekedSlot ShmRing::beginRead(const SlotHandle& handle) const
{
    SlotHeader* slot = slotHeader(handle.slot_index);

    const uint64_t seq = slot->seq.load(std::memory_order_seq_cst);
    // Check if the slot is valid(even seq means it's ready for reading) and the sequence number matches
    if (seq != handle.seq || (seq & 1U) != 0) {
        return {};
    }

    return {slotPayload(handle.slot_index), handle.size};
}

bool ShmRing::finishRead(const SlotHandle& handle) const
{
    SlotHeader* slot = slotHeader(handle.slot_index);
    return slot->seq.load(std::memory_order_seq_cst) == handle.seq;
}

bool ShmRing::read(const SlotHandle& handle, std::string& out) const
{
    const PeekedSlot peeked = beginRead(handle);
    if (!peeked.payload) {
        return false;
    }

    out.assign(peeked.payload, peeked.size);
    return finishRead(handle);
}

} // namespace trans
} // namespace rf
