Average by using AggregationByKey Transformation:
val sumdata  = finalRDD.map(x=>(x.split('\t')(5),x.split('\t')(6) ))
val avg = sumdata.AggregateByKey( 0, (x,y) => x+y, p1+p2)

