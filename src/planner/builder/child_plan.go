/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package builder

// ChildType type.
type ChildType string

const (
	// ChildTypeOrderby enum.
	ChildTypeOrderby ChildType = "ChildTypeOrderby"

	// ChildTypeLimit enum.
	ChildTypeLimit ChildType = "ChildTypeLimit"

	// ChildTypeAggregate enum.
	ChildTypeAggregate ChildType = "ChildTypeAggregate"
)

// ChildPlan interface.
type ChildPlan interface {
	Build() error
	Type() ChildType
	JSON() string
}

func childInfo(childs []ChildPlan) (aggregate []Aggregator, gatherMerge []OrderBy, lim *limit) {
	for _, sub := range childs {
		switch sub.Type() {
		case ChildTypeAggregate:
			plan := sub.(*AggregatePlan)
			aggregate = plan.NormalAggregators()
			aggregate = append(aggregate, plan.GroupAggregators()...)
		case ChildTypeOrderby:
			plan := sub.(*OrderByPlan)
			gatherMerge = plan.OrderBys
		case ChildTypeLimit:
			plan := sub.(*LimitPlan)
			lim = &limit{Offset: plan.Offset, Limit: plan.Limit}
		}
	}
	return
}
