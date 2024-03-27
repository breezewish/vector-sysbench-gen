package utils

import "math/rand/v2"

func RandRange(min, max int) int {
	return rand.IntN(max-min) + min
}

func RandFloat32Range(min, max float32) float32 {
	return min + rand.Float32()*(max-min)
}
