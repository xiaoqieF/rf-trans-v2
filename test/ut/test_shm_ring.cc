#include <string>

#include <unistd.h>

#include <gtest/gtest.h>

#include "trans/details/shm_ring.hpp"

using rf::trans::ShmRing;

namespace
{
std::string uniqueName(const char* test_name)
{
    return std::string("/rf_trans_ut_") + test_name + "_" + std::to_string(getpid());
}
} // namespace

TEST(shm_ring, writerReadsBackItsOwnWrite)
{
    ShmRing writer(uniqueName("roundtrip"), 64);
    ASSERT_TRUE(writer.createAsWriter());

    const std::string payload = "hello shm";
    const auto handle = writer.write(payload.data(), payload.size());

    std::string out;
    ASSERT_TRUE(writer.read(handle, out));
    EXPECT_EQ(out, payload);
}

TEST(shm_ring, readerOpensWriterSegmentAndSeesData)
{
    const std::string name = uniqueName("cross_instance");
    ShmRing writer(name, 64);
    ASSERT_TRUE(writer.createAsWriter());

    const std::string payload = "cross process payload";
    const auto handle = writer.write(payload.data(), payload.size());

    // Reader constructed independently, mirroring how MessageRuntime opens a
    // ring it did not create: slot size/count come from the segment header.
    ShmRing reader(name, 0);
    ASSERT_TRUE(reader.openAsReader());
    EXPECT_EQ(reader.slotSize(), writer.slotSize());
    EXPECT_EQ(reader.slotCount(), writer.slotCount());

    std::string out;
    ASSERT_TRUE(reader.read(handle, out));
    EXPECT_EQ(out, payload);
}

TEST(shm_ring, readingOverwrittenSlotFails)
{
    // Only two slots, so the third write recycles slot 0 and invalidates the
    // handle from the first write.
    ShmRing writer(uniqueName("overwrite"), 64, 2);
    ASSERT_TRUE(writer.createAsWriter());

    const auto stale_handle = writer.write("first", 5);
    writer.write("second", 6);
    writer.write("third", 5);

    std::string out;
    EXPECT_FALSE(writer.read(stale_handle, out));
}

TEST(shm_ring, writeLargerThanSlotSizeFails)
{
    ShmRing writer(uniqueName("oversized"), 8);
    ASSERT_TRUE(writer.createAsWriter());

    const std::string too_big(9, 'x');
    const auto failed_handle = writer.write(too_big.data(), too_big.size());
    // write() bails out before touching write_cursor_ on failure, so a
    // successful write always carries a non-zero seq (the seqlock's odd/even
    // pair starts at 1); a zero seq unambiguously marks the failed attempt.
    EXPECT_EQ(failed_handle.seq, 0u);

    const std::string ok(8, 'y');
    const auto handle = writer.write(ok.data(), ok.size());
    EXPECT_EQ(handle.slot_index, 0u);
    EXPECT_NE(handle.seq, 0u);

    std::string out;
    ASSERT_TRUE(writer.read(handle, out));
    EXPECT_EQ(out, ok);
}

TEST(shm_ring, writerDestructorUnlinksSegment)
{
    const std::string name = uniqueName("unlink");
    {
        ShmRing writer(name, 64);
        ASSERT_TRUE(writer.createAsWriter());
    }

    ShmRing reader(name, 0);
    EXPECT_FALSE(reader.openAsReader());
}

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
