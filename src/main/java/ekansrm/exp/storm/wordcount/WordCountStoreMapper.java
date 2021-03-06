package ekansrm.exp.storm.wordcount;

import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.tuple.ITuple;

class WordCountStoreMapper implements RedisStoreMapper {
  private RedisDataTypeDescription description;
  private final String hashKey = "wordCount";

  public WordCountStoreMapper() {
    description = new RedisDataTypeDescription(
      RedisDataTypeDescription.RedisDataType.HASH, hashKey);
  }

  @Override
  public RedisDataTypeDescription getDataTypeDescription() {
    return description;
  }

  @Override
  public String getKeyFromTuple(ITuple tuple) {
    return tuple.getStringByField("word");
  }

  @Override
  public String getValueFromTuple(ITuple tuple) {
    return tuple.getIntegerByField("count").toString();
  }

}
