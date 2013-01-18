package titan.gateway.setup;

import titan.sys.data.SysKey;

public interface PartitionKeyFactory {

	public Long getPartitionKey(SysKey dataKey, String setName, int totalPartitions);
	public Long getPartitionKey(int partition, String setName, int totalPartitions);
}
