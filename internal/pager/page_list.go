package pager

import "container/list"

type PageInterval struct {
	Start PagePointer
	End   PagePointer
}

type PageList struct {
	intervals *list.List
}

func NewPageList(intervals ...PageInterval) PageList {
	list := PageList{intervals: list.New()}

	for _, interval := range intervals {
		list.intervals.PushBack(PageInterval{Start: interval.Start, End: interval.End})
	}

	return list
}

func (list PageList) Clone() PageList {
	return NewPageList(list.Pages()...)
}

func (list PageList) Add(page PagePointer) {
	for el := list.intervals.Front(); el != nil; el = el.Next() {
		interval := el.Value.(PageInterval)

		if page >= interval.Start && page <= interval.End {
			return
		}

		if page == interval.End+1 {
			el.Value = PageInterval{Start: interval.Start, End: page}
			list.tryMerge(el)
			return
		}

		if page == interval.Start-1 {
			el.Value = PageInterval{Start: page, End: interval.End}
			list.tryMerge(el)
			return
		}

		if page < interval.Start {
			list.intervals.InsertBefore(PageInterval{Start: page, End: page}, el)
			return
		}
	}

	list.intervals.PushBack(PageInterval{Start: page, End: page})
}

func (list PageList) AddMany(intervals []PageInterval) {
	for _, interval := range intervals {
		list.addInterval(interval)
	}
}

func (list PageList) Pop() (PagePointer, bool) {
	if list.Empty() {
		return NULL_PAGE, false
	}

	front := list.intervals.Front()
	interval := front.Value.(PageInterval)
	page := interval.Start

	if interval.Start == interval.End {
		list.intervals.Remove(front)
	} else {
		interval.Start++
		front.Value = interval
	}

	return page, true
}

func (list PageList) Has(page PagePointer) bool {
	for el := list.intervals.Front(); el != nil; el = el.Next() {
		interval := el.Value.(PageInterval)

		if page >= interval.Start && page <= interval.End {
			return true
		}

		if page < interval.Start {
			return false
		}
	}

	return false
}

func (list PageList) Pages() []PageInterval {
	intervals := make([]PageInterval, 0, list.intervals.Len())

	for el := list.intervals.Front(); el != nil; el = el.Next() {
		intervals = append(intervals, el.Value.(PageInterval))
	}

	return intervals
}

func (list PageList) Empty() bool {
	return list.intervals.Len() == 0
}

func (list PageList) addInterval(interval PageInterval) {
	for el := list.intervals.Front(); el != nil; el = el.Next() {
		current := el.Value.(PageInterval)

		if interval.End < current.Start {
			list.intervals.InsertBefore(PageInterval{Start: interval.Start, End: interval.End}, el)
			return
		}

		if current.Start > interval.End {
			continue
		}

		if current.Start < interval.Start {
			interval.Start = current.Start
		}

		if current.End > interval.End {
			interval.End = current.End
		}

		list.tryMerge(el)
		return
	}

	list.intervals.PushBack(PageInterval{Start: interval.Start, End: interval.End})
}

func (list PageList) tryMerge(el *list.Element) {
	interval := el.Value.(PageInterval)

	if prev := el.Prev(); prev != nil && prev.Value.(PageInterval).End+1 == interval.Start {
		prev.Value = PageInterval{Start: prev.Value.(PageInterval).Start, End: interval.End}
		list.intervals.Remove(el)
		return
	}

	if next := el.Next(); next != nil && interval.End+1 == next.Value.(PageInterval).Start {
		el.Value = PageInterval{Start: interval.Start, End: next.Value.(PageInterval).End}
		list.intervals.Remove(next)
		return
	}
}
