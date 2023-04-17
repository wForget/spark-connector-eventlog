package  org.apache.spark.sql.connector.eventlog

import org.apache.spark.sql.catalyst.json.JSONOptionsInRead
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

class EventLogJSONOptionsInRead(
    @transient override val parameters: CaseInsensitiveMap[String],
    defaultTimeZoneId: String,
    defaultColumnNameOfCorruptRecord: String) extends JSONOptionsInRead(parameters,
  defaultTimeZoneId, defaultColumnNameOfCorruptRecord)