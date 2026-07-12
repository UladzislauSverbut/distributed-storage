package wal

import (
	"fmt"
	"os"
	"sort"
)

type segmentDescriptor struct {
	start     EntryIndex
	name      string
	directory string
}

func (descriptor segmentDescriptor) path() string {
	return descriptor.directory + "/" + descriptor.name
}

func readSegments(directory string) ([]segmentDescriptor, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("WAL: failed to read directory %q: %w", directory, err)
	}

	var descriptors []segmentDescriptor
	for _, entry := range entries {
		if index, err := parseSegmentName(entry.Name()); err == nil {
			descriptors = append(descriptors, segmentDescriptor{start: index, name: entry.Name(), directory: directory})
		}
	}

	sort.Slice(descriptors, func(i, j int) bool { return descriptors[i].name < descriptors[j].name })

	return descriptors, nil
}

func contains(segments []segmentDescriptor, index EntryIndex) bool {
	for _, desc := range segments {
		if desc.start <= index {
			return true
		}
	}
	return false
}
