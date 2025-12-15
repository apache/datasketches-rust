package tools

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/datasketches-go/hll"
	"github.com/stretchr/testify/require"
)

const defaultLgK = 12

func TestGenerateGoFile(t *testing.T) {
	path, err := os.Getwd()
	require.NoError(t, err)

	workspace := filepath.Join(path, "..", "..", "..")
	target := filepath.Join(workspace, "tests", "serialization_test_data", "go_generated_files")

	nArr := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
	for _, n := range nArr {
		hll4, err := hll.NewHllSketch(defaultLgK, hll.TgtHllTypeHll4)
		require.NoError(t, err)
		hll6, err := hll.NewHllSketch(defaultLgK, hll.TgtHllTypeHll6)
		require.NoError(t, err)
		hll8, err := hll.NewHllSketch(defaultLgK, hll.TgtHllTypeHll8)
		require.NoError(t, err)

		for i := 0; i < n; i++ {
			require.NoError(t, hll4.UpdateUInt64(uint64(i)))
			require.NoError(t, hll6.UpdateUInt64(uint64(i)))
			require.NoError(t, hll8.UpdateUInt64(uint64(i)))
		}
		err = os.MkdirAll(target, os.ModePerm)
		require.NoError(t, err)

		sl4, err := hll4.ToCompactSlice()
		require.NoError(t, err)
		err = os.WriteFile(fmt.Sprintf("%s/hll4_n%d_go.sk", target, n), sl4, 0644)
		require.NoError(t, err)

		sl6, err := hll6.ToCompactSlice()
		require.NoError(t, err)
		err = os.WriteFile(fmt.Sprintf("%s/hll6_n%d_go.sk", target, n), sl6, 0644)
		require.NoError(t, err)

		sl8, err := hll8.ToCompactSlice()
		require.NoError(t, err)
		err = os.WriteFile(fmt.Sprintf("%s/hll8_n%d_go.sk", target, n), sl8, 0644)
		require.NoError(t, err)
	}
}
