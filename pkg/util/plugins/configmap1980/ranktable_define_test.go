package configmap1980

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompressAndDecompress(t *testing.T) {
	// 测试数据
	input := []byte("test data for compression and decompression")

	// 压缩数据
	compressed, err := Compress(input)
	assert.NoError(t, err, "Compress should not return an error")
	assert.NotEmpty(t, compressed, "Compressed data should not be empty")

	// 解压缩数据
	decompressed, err := Decompress(compressed)
	assert.NoError(t, err, "Decompress should not return an error")
	assert.Equal(t, input, decompressed, "Decompressed data should match the original input")
}
