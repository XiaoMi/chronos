namespace java com.xiaomi.infra.chronos.generated

service ChronosService{
  i64 getTimestamp(),
  i64 getTimestamps(1:i32 range)
}