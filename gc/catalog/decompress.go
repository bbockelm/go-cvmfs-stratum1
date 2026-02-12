package catalog

import (
	"compress/zlib"
	"io"
	"os"
)

// decompressZlibStream reads a zlib-compressed stream from src and writes
// the decompressed data to dst.
func decompressZlibStream(src *os.File, dst *os.File) error {
	reader, err := zlib.NewReader(src)
	if err != nil {
		return err
	}
	defer reader.Close()

	_, err = io.Copy(dst, reader)
	return err
}
