/*
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package common

// Max returns the larger of x and y.
func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

// Min returns the smaller of x and y.
func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
