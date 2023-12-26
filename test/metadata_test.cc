#include <metadata.h>

int main () 
{
    size_t num_types = 2;
    std::vector<size_t> type_order = {1,53};
	std::vector<size_t> byte_offsets = {0,100,200,300,400};

    HawaiiMetadataPage hawaii_metadata_page(num_types, type_order, byte_offsets);

    assert (hawaii_metadata_page.num_types() == num_types);
    assert (hawaii_metadata_page.type_order() == type_order);
    assert (hawaii_metadata_page.byte_offsets() == byte_offsets);

    std::string compressed_metadata_page = hawaii_metadata_page.compress();
    HawaiiMetadataPage decompressed_metadata_page(compressed_metadata_page);

    assert (decompressed_metadata_page.num_types() == num_types);
    assert (decompressed_metadata_page.type_order() == type_order);
    assert (decompressed_metadata_page.byte_offsets() == byte_offsets);
}
