#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iomanip>
#include <sstream>
#include <string>

namespace rf
{
namespace trans
{

/// \brief Derive a POSIX shm object name from a topic and the publishing
/// process' uuid. Both may contain characters shm_open rejects (e.g. '/'),
/// so the topic is hashed rather than used verbatim.
inline std::string makeShmName(const std::string& topic, const std::string& process_uuid)
{
    std::ostringstream oss;
    oss << "/rf_shm_" << std::hex << std::hash<std::string>{}(topic)
        << "_" << process_uuid.substr(0, 8);
    return oss.str();
}

/// \brief Prefix byte prepended to the msg_type frame on the wire when the
/// data frame carries a ShmDescriptor rather than the serialized message
/// itself. A plain protobuf full name never starts with a control character,
/// so its absence unambiguously means "inline data, as before" and leaves
/// non-shm topics wire-compatible with tools like TopicSniffer that only
/// understand the inline format.
constexpr char kShmMsgTypeMarker = '\x01';

/// \brief Wire-format descriptor sent over the existing ZMQ control channel
/// in place of the serialized payload when a message is delivered through
/// shared memory. `shm_name` is the POSIX shm object name the writer created
/// (readers use it verbatim, they never compute it themselves).
struct ShmDescriptor
{
    char shm_name[64];
    uint32_t slot_index;
    uint64_t seq;
    uint32_t size;
};

/// \brief Fixed-slot ring buffer backed by POSIX shared memory, used to hand
/// large message payloads to same-host subscribers without the extra
/// serialize/network copy hop the regular transport uses.
///
/// Each slot is guarded by a seqlock: the writer marks a slot odd while it is
/// copying data in and even once the slot is published; a reader rejects a
/// slot whose sequence is odd, or that changed while it was being copied out.
/// This is a best-effort scheme, the same way the rest of the transport
/// already drops messages under backpressure: a slow reader can lose a
/// message to a fast writer that wraps the ring before the read completes.
class ShmRing
{
public:
    struct SlotHandle
    {
        uint32_t slot_index{0};
        uint64_t seq{0};
        uint32_t size{0};
    };

    /// \brief A slot reserved via reserveWrite(), not yet published to
    /// readers. `payload` points directly into the shared memory segment;
    /// the caller writes the message bytes there in place (e.g. protobuf
    /// SerializeToArray) instead of copying through a scratch buffer first.
    /// `payload` is null when reservation failed.
    struct ReservedSlot
    {
        uint32_t slot_index{0};
        uint64_t begin_seq{0};
        char* payload{nullptr};
    };

    static constexpr size_t kShmGranularity = 4 * 1024;    // 4k align
    static constexpr size_t kDefaultSlotCount = 4;

    ShmRing(std::string name, size_t slot_size, size_t slot_count = kDefaultSlotCount);
    ~ShmRing();

    ShmRing(const ShmRing&) = delete;
    ShmRing& operator=(const ShmRing&) = delete;

    /// \brief Create (or re-create) the backing shared memory segment for writing.
    /// Only the owning writer should call this; the destructor unlinks the
    /// segment when it was created by this instance.
    bool createAsWriter();

    /// \brief Open an existing backing shared memory segment for reading.
    bool openAsReader();

    /// \brief Reserve the next slot and mark it mid-write (odd seq), without
    /// copying any data yet. `size` must not exceed slotSize(); the caller is
    /// expected to have checked that already. Must be followed by a matching
    /// commitWrite() once the payload bytes have been written to
    /// `ReservedSlot::payload`. If the caller abandons the reservation
    /// without committing (e.g. because serialization into `payload` failed),
    /// the slot is simply left stuck at an odd seq until the ring wraps
    /// around and reuses it; since no handle for an uncommitted slot is ever
    /// handed to a reader, this is harmless.
    ReservedSlot reserveWrite(size_t size);

    /// \brief Publish a slot reserved via reserveWrite(), marking it even
    /// (readable) again. Returns a handle describing where the data landed,
    /// to be forwarded to readers out-of-band (e.g. over the existing
    /// control channel).
    SlotHandle commitWrite(const ReservedSlot& reserved, size_t size);

    /// \brief Copy `size` bytes into the next slot. `size` must not exceed
    /// slotSize(); the caller is expected to have checked that already.
    /// Returns a handle describing where the data landed, to be forwarded to
    /// readers out-of-band (e.g. over the existing control channel).
    /// Convenience wrapper around reserveWrite()+commitWrite() for callers
    /// that already have the data in a contiguous buffer.
    SlotHandle write(const void* data, size_t size);

    /// \brief Result of beginRead(): a direct pointer into shared memory for
    /// the slot described by the handle passed in, or a null `payload` if
    /// the slot is no longer valid for that handle (overwritten, or
    /// mid-write). Lets the caller consume the payload in place (e.g.
    /// protobuf ParseFromArray) instead of copying it out first.
    struct PeekedSlot
    {
        const char* payload{nullptr};
        uint32_t size{0};
    };

    /// \brief Begin a copy-free read of the slot described by `handle`.
    /// Must be paired with finishRead(handle) once the caller is done
    /// consuming `PeekedSlot::payload`; if that returns false, discard
    /// whatever was produced from it; the writer overwrote the slot while it
    /// was being consumed, so parsing may have run over a torn mix of old
    /// and new bytes with no guarantee of well-formedness. This is the same
    /// seqlock guarantee read() provides, just applied around the caller's
    /// own consumption of the payload instead of around a memcpy.
    PeekedSlot beginRead(const SlotHandle& handle) const;

    /// \brief Complete a speculative read started with beginRead(). Returns
    /// false if the writer overwrote the slot while the caller was consuming
    /// it, in which case the caller must discard whatever it produced.
    bool finishRead(const SlotHandle& handle) const;

    /// \brief Read the slot described by `handle` into `out`. Returns false
    /// if the slot was overwritten (or is mid-write) by the time it's read;
    /// the caller should treat that the same as a dropped message.
    /// Convenience wrapper around beginRead()+finishRead() for callers that
    /// want an owned copy rather than parsing in place.
    bool read(const SlotHandle& handle, std::string& out) const;

    const std::string& name() const { return name_; }
    size_t slotSize() const { return slot_size_; }
    size_t slotCount() const { return slot_count_; }

private:
    struct RingHeader
    {
        uint32_t magic;
        uint32_t slot_count;
        uint32_t slot_size;
        uint32_t reserved;
    };

    struct SlotHeader
    {
        std::atomic<uint64_t> seq;
        uint32_t size;
        uint32_t reserved;
    };

    static constexpr uint32_t kMagic = 0x52465348; // 'RFSH'

    bool openShm(bool create);
    size_t slotStride() const;
    RingHeader* ringHeader() const;
    SlotHeader* slotHeader(uint32_t index) const;
    char* slotPayload(uint32_t index) const;

    std::string name_;
    size_t slot_size_;
    size_t slot_count_;
    size_t region_size_{0};

    int fd_{-1};
    void* base_{nullptr};
    bool is_writer_{false};

    std::atomic<uint64_t> write_cursor_{0};
};

} // namespace trans
} // namespace rf
