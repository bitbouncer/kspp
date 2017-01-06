filter  
	filter_by_key   <K, V>          ->     <K, V>					TBD
	filter_by_value <K, V>          ->     <K, V>                   TBD
	a > 10?
	
transform
	??? transform       <K, V>      ->     <K1,V1>                  ???
	transform_by_value  <K, V>      ->     <K, V1>                  OK  
    flat_map            <K, V>      ->     {<K1,V1>, ..., <K1,V1>} 	OK


repartition
	repartition <K, V> is this by foreign key??                     OK
	group by???

	
aggregate 
	count_by_key  <K>            ->     <K, size_t>  OK
	sum_by_key    <K, numeric>   ->     <K, numeric>               TBD
	avg_by_key    <K, numeric>   ->     <K, numeric>               TBD
	99%_by_key 
	windowed!!!
	
	
join
    left_join stream<K, Vs> x table<K, Vt>  ->  stream <K, Vr>      OK
	windowed!!!                                                     TBD
	
	
external_sink
    external_sink <K, V>
	external_unordered_sink <K, V>
	external_unordered_sink <K, V> with retry

external_source
	source<K,V> -> source<Kr, Vr>
	source<K,V> 

external_transform_ordered with retry
external_transform_unordered with retry
		windowed
		

	
	
	