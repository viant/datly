/* {"URI":"basic/bars/", "DataFormat":"tabular", "TabularJSON":{"FloatPrecision":"8"}} */

#set( $_ = $Data<?>(output/view).WithTag('anonymous:"true"'))

SELECT bar.*
FROM (SELECT * FROM BAR t ) bar
