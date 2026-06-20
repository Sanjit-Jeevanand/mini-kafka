package consumergroup

import "sort"

func stickyAssign(
	numPartitions int,
	members map[string]struct{},
	current map[string][]int,
) map[string][]int {
	n := len(members)
	if n == 0 {
		return make(map[string][]int)
	}

	next := make(map[string][]int, n)
	owned := make(map[int]bool, numPartitions)

	for memberID := range members {
		next[memberID] = nil
		for _, p := range current[memberID] {
			next[memberID] = append(next[memberID], p)
			owned[p] = true
		}
	}

	var pool []int
	for p := 0; p < numPartitions; p++ {
		if !owned[p] {
			pool = append(pool, p)
		}
	}

	idealLow := numPartitions / n
	extra := numPartitions % n

	desc := membersByLoad(next)
	for i, j := 0, len(desc)-1; i < j; i, j = i+1, j-1 {
		desc[i], desc[j] = desc[j], desc[i]
	}
	extraLeft := extra
	for _, m := range desc {
		max := idealLow
		if extraLeft > 0 {
			max = idealLow + 1
			extraLeft--
		}
		parts := next[m]
		sort.Ints(parts)
		for len(parts) > max {
			pool = append(pool, parts[len(parts)-1])
			parts = parts[:len(parts)-1]
		}
		next[m] = parts
	}

	if len(pool) == 0 {
		return next
	}

	sort.Ints(pool)
	asc := membersByLoad(next)
	for _, p := range pool {
		m := asc[0]
		next[m] = append(next[m], p)
		sort.Slice(asc, func(a, b int) bool {
			if len(next[asc[a]]) != len(next[asc[b]]) {
				return len(next[asc[a]]) < len(next[asc[b]])
			}
			return asc[a] < asc[b]
		})
	}

	return next
}

func membersByLoad(assignments map[string][]int) []string {
	members := make([]string, 0, len(assignments))
	for id := range assignments {
		members = append(members, id)
	}
	sort.Slice(members, func(i, j int) bool {
		li, lj := len(assignments[members[i]]), len(assignments[members[j]])
		if li != lj {
			return li < lj
		}
		return members[i] < members[j]
	})
	return members
}
