// ORM class for table 'null'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Fri Jan 08 00:45:35 UTC 2016
// For connector: org.apache.sqoop.manager.MySQLManager
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import com.cloudera.sqoop.lib.JdbcWritableBridge;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.FieldFormatter;
import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.lib.BooleanParser;
import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class QueryResult extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  protected ResultSet __cur_result_set;
  private Integer id;
  public Integer get_id() {
    return id;
  }
  public void set_id(Integer id) {
    this.id = id;
  }
  public QueryResult with_id(Integer id) {
    this.id = id;
    return this;
  }
  private String email_address;
  public String get_email_address() {
    return email_address;
  }
  public void set_email_address(String email_address) {
    this.email_address = email_address;
  }
  public QueryResult with_email_address(String email_address) {
    this.email_address = email_address;
    return this;
  }
  private Integer channel_id;
  public Integer get_channel_id() {
    return channel_id;
  }
  public void set_channel_id(Integer channel_id) {
    this.channel_id = channel_id;
  }
  public QueryResult with_channel_id(Integer channel_id) {
    this.channel_id = channel_id;
    return this;
  }
  private Integer user_profile_id;
  public Integer get_user_profile_id() {
    return user_profile_id;
  }
  public void set_user_profile_id(Integer user_profile_id) {
    this.user_profile_id = user_profile_id;
  }
  public QueryResult with_user_profile_id(Integer user_profile_id) {
    this.user_profile_id = user_profile_id;
    return this;
  }
  private String status;
  public String get_status() {
    return status;
  }
  public void set_status(String status) {
    this.status = status;
  }
  public QueryResult with_status(String status) {
    this.status = status;
    return this;
  }
  private String cast__created_at_aschar_;
  public String get_cast__created_at_aschar_() {
    return cast__created_at_aschar_;
  }
  public void set_cast__created_at_aschar_(String cast__created_at_aschar_) {
    this.cast__created_at_aschar_ = cast__created_at_aschar_;
  }
  public QueryResult with_cast__created_at_aschar_(String cast__created_at_aschar_) {
    this.cast__created_at_aschar_ = cast__created_at_aschar_;
    return this;
  }
  private String cast__updated_at_aschar_;
  public String get_cast__updated_at_aschar_() {
    return cast__updated_at_aschar_;
  }
  public void set_cast__updated_at_aschar_(String cast__updated_at_aschar_) {
    this.cast__updated_at_aschar_ = cast__updated_at_aschar_;
  }
  public QueryResult with_cast__updated_at_aschar_(String cast__updated_at_aschar_) {
    this.cast__updated_at_aschar_ = cast__updated_at_aschar_;
    return this;
  }
  private String cast__opt_type_aschar_;
  public String get_cast__opt_type_aschar_() {
    return cast__opt_type_aschar_;
  }
  public void set_cast__opt_type_aschar_(String cast__opt_type_aschar_) {
    this.cast__opt_type_aschar_ = cast__opt_type_aschar_;
  }
  public QueryResult with_cast__opt_type_aschar_(String cast__opt_type_aschar_) {
    this.cast__opt_type_aschar_ = cast__opt_type_aschar_;
    return this;
  }
  private String subscribe_ip;
  public String get_subscribe_ip() {
    return subscribe_ip;
  }
  public void set_subscribe_ip(String subscribe_ip) {
    this.subscribe_ip = subscribe_ip;
  }
  public QueryResult with_subscribe_ip(String subscribe_ip) {
    this.subscribe_ip = subscribe_ip;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof QueryResult)) {
      return false;
    }
    QueryResult that = (QueryResult) o;
    boolean equal = true;
    equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
    equal = equal && (this.email_address == null ? that.email_address == null : this.email_address.equals(that.email_address));
    equal = equal && (this.channel_id == null ? that.channel_id == null : this.channel_id.equals(that.channel_id));
    equal = equal && (this.user_profile_id == null ? that.user_profile_id == null : this.user_profile_id.equals(that.user_profile_id));
    equal = equal && (this.status == null ? that.status == null : this.status.equals(that.status));
    equal = equal && (this.cast__created_at_aschar_ == null ? that.cast__created_at_aschar_ == null : this.cast__created_at_aschar_.equals(that.cast__created_at_aschar_));
    equal = equal && (this.cast__updated_at_aschar_ == null ? that.cast__updated_at_aschar_ == null : this.cast__updated_at_aschar_.equals(that.cast__updated_at_aschar_));
    equal = equal && (this.cast__opt_type_aschar_ == null ? that.cast__opt_type_aschar_ == null : this.cast__opt_type_aschar_.equals(that.cast__opt_type_aschar_));
    equal = equal && (this.subscribe_ip == null ? that.subscribe_ip == null : this.subscribe_ip.equals(that.subscribe_ip));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.id = JdbcWritableBridge.readInteger(1, __dbResults);
    this.email_address = JdbcWritableBridge.readString(2, __dbResults);
    this.channel_id = JdbcWritableBridge.readInteger(3, __dbResults);
    this.user_profile_id = JdbcWritableBridge.readInteger(4, __dbResults);
    this.status = JdbcWritableBridge.readString(5, __dbResults);
    this.cast__created_at_aschar_ = JdbcWritableBridge.readString(6, __dbResults);
    this.cast__updated_at_aschar_ = JdbcWritableBridge.readString(7, __dbResults);
    this.cast__opt_type_aschar_ = JdbcWritableBridge.readString(8, __dbResults);
    this.subscribe_ip = JdbcWritableBridge.readString(9, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeInteger(id, 1 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeString(email_address, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(channel_id, 3 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(user_profile_id, 4 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeString(status, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(cast__created_at_aschar_, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(cast__updated_at_aschar_, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(cast__opt_type_aschar_, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(subscribe_ip, 9 + __off, 12, __dbStmt);
    return 9;
  }
  public void readFields(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.id = null;
    } else {
    this.id = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.email_address = null;
    } else {
    this.email_address = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.channel_id = null;
    } else {
    this.channel_id = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.user_profile_id = null;
    } else {
    this.user_profile_id = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.status = null;
    } else {
    this.status = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.cast__created_at_aschar_ = null;
    } else {
    this.cast__created_at_aschar_ = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.cast__updated_at_aschar_ = null;
    } else {
    this.cast__updated_at_aschar_ = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.cast__opt_type_aschar_ = null;
    } else {
    this.cast__opt_type_aschar_ = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.subscribe_ip = null;
    } else {
    this.subscribe_ip = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.id);
    }
    if (null == this.email_address) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, email_address);
    }
    if (null == this.channel_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.channel_id);
    }
    if (null == this.user_profile_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.user_profile_id);
    }
    if (null == this.status) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, status);
    }
    if (null == this.cast__created_at_aschar_) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, cast__created_at_aschar_);
    }
    if (null == this.cast__updated_at_aschar_) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, cast__updated_at_aschar_);
    }
    if (null == this.cast__opt_type_aschar_) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, cast__opt_type_aschar_);
    }
    if (null == this.subscribe_ip) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, subscribe_ip);
    }
  }
  private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 1, (char) 10, (char) 0, (char) 0, false);
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
    __sb.append(FieldFormatter.escapeAndEnclose(id==null?"":"" + id, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(email_address==null?"":email_address, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(channel_id==null?"":"" + channel_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(user_profile_id==null?"":"" + user_profile_id, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(status==null?"":status, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(cast__created_at_aschar_==null?"":cast__created_at_aschar_, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(cast__updated_at_aschar_==null?"":cast__updated_at_aschar_, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(cast__opt_type_aschar_==null?"":cast__opt_type_aschar_, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(subscribe_ip==null?"":subscribe_ip, " ", delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 1, (char) 10, (char) 0, (char) 0, false);
  private RecordParser __parser;
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  private void __loadFromFields(List<String> fields) {
    Iterator<String> __it = fields.listIterator();
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.id = null; } else {
      this.id = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.email_address = null; } else {
      this.email_address = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.channel_id = null; } else {
      this.channel_id = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.user_profile_id = null; } else {
      this.user_profile_id = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.status = null; } else {
      this.status = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.cast__created_at_aschar_ = null; } else {
      this.cast__created_at_aschar_ = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.cast__updated_at_aschar_ = null; } else {
      this.cast__updated_at_aschar_ = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.cast__opt_type_aschar_ = null; } else {
      this.cast__opt_type_aschar_ = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.subscribe_ip = null; } else {
      this.subscribe_ip = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    QueryResult o = (QueryResult) super.clone();
    return o;
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new TreeMap<String, Object>();
    __sqoop$field_map.put("id", this.id);
    __sqoop$field_map.put("email_address", this.email_address);
    __sqoop$field_map.put("channel_id", this.channel_id);
    __sqoop$field_map.put("user_profile_id", this.user_profile_id);
    __sqoop$field_map.put("status", this.status);
    __sqoop$field_map.put("cast__created_at_aschar_", this.cast__created_at_aschar_);
    __sqoop$field_map.put("cast__updated_at_aschar_", this.cast__updated_at_aschar_);
    __sqoop$field_map.put("cast__opt_type_aschar_", this.cast__opt_type_aschar_);
    __sqoop$field_map.put("subscribe_ip", this.subscribe_ip);
    return __sqoop$field_map;
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if ("id".equals(__fieldName)) {
      this.id = (Integer) __fieldVal;
    }
    else    if ("email_address".equals(__fieldName)) {
      this.email_address = (String) __fieldVal;
    }
    else    if ("channel_id".equals(__fieldName)) {
      this.channel_id = (Integer) __fieldVal;
    }
    else    if ("user_profile_id".equals(__fieldName)) {
      this.user_profile_id = (Integer) __fieldVal;
    }
    else    if ("status".equals(__fieldName)) {
      this.status = (String) __fieldVal;
    }
    else    if ("cast__created_at_aschar_".equals(__fieldName)) {
      this.cast__created_at_aschar_ = (String) __fieldVal;
    }
    else    if ("cast__updated_at_aschar_".equals(__fieldName)) {
      this.cast__updated_at_aschar_ = (String) __fieldVal;
    }
    else    if ("cast__opt_type_aschar_".equals(__fieldName)) {
      this.cast__opt_type_aschar_ = (String) __fieldVal;
    }
    else    if ("subscribe_ip".equals(__fieldName)) {
      this.subscribe_ip = (String) __fieldVal;
    }
    else {
      throw new RuntimeException("No such field: " + __fieldName);
    }
  }
}
