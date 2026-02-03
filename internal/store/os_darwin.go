package store

import (
	"os"

	"golang.org/x/sys/unix"
)

var page_size = unix.Getpagesize()

func increaseFileSize(file *os.File, allocatedFileSize int) (int, error) {
	alignedFileSize := int64((allocatedFileSize + page_size - 1) & ^(page_size - 1))

	if err := unix.FcntlFstore(file.Fd(), unix.F_ALLOCATECONTIG, &unix.Fstore_t{
		Flags:   unix.F_ALLOCATECONTIG,
		Posmode: unix.F_PEOFPOSMODE,
		Length:  alignedFileSize,
	}); err != nil {
		return 0, err
	}

	if err := file.Truncate(alignedFileSize); err != nil {
		return 0, err
	}

	return int(alignedFileSize), nil
}

func mapFileToMemory(file *os.File, offset int64, size int) (data []byte, err error) {
	return unix.Mmap(int(file.Fd()), offset, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
}
