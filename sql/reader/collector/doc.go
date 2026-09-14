// Package collector owns reader-compiled relation graphs and typed row assembly.
// SQLX supplies typed rows; collector reconciles parent-child relations without
// introducing another scanner or row-cache model.
package collector
