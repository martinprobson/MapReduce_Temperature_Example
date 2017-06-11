# MapReduce Temperature Example - Unix version

## Summary
This is an implementation of the Map/Reduce logic using Unix tools.
To run use the following command line: -

```bash
	./max_temperature_mapper.sh | sort | ./max_temperature_reducer.awk
```

This equates to the *MAP ->> SHUFFLE-SORT ->> REDUCE* cycle in Hadoop (although the shuffle is obviously not required).
