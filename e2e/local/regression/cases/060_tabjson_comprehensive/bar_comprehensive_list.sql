/* {"URI":"comprehensive/bars/", "DataFormat":"tabular", "TabularJSON":{"FloatPrecision":"-1"}} */


#set( $_ = $Data<?>(output/view).Cardinality('Many'))
#set($_ =  $Status<?>(output/status))

SELECT bar.*
FROM (SELECT * FROM BAR t ) bar
