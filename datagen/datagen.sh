

function create_connector {
	command=`cat template | sed s/CONNECTOR_NAME/$1/ | sed s/DATA_SET/$2/g`
	curl -X "POST" "http://localhost:8088/ksql" \
	-H "Accept: application/vnd.ksql.v1+json" \
	-d '{
		"ksql": "'"drop connector $1;$command"'",
		"streamsProperties": {}
	}'
}

datasets='clickstream_codes clickstream clickstream_users orders ratings users users_ pageviews stock_trades inventory product'
if [ $# -lt 1 ]
then
	echo 'Please enter a dataset. Your choices are:'
	for ds in $datasets; do
		echo $ds	
	done
	exit
fi

create_connector "$1_gen" $1
