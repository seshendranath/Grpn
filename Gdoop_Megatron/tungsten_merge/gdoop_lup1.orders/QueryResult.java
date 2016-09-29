// ORM class for table 'null'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Fri Apr 01 17:45:22 UTC 2016
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
  private String cast__uid_aschar_;
  public String get_cast__uid_aschar_() {
    return cast__uid_aschar_;
  }
  public void set_cast__uid_aschar_(String cast__uid_aschar_) {
    this.cast__uid_aschar_ = cast__uid_aschar_;
  }
  public QueryResult with_cast__uid_aschar_(String cast__uid_aschar_) {
    this.cast__uid_aschar_ = cast__uid_aschar_;
    return this;
  }
  private Integer purchaser_id;
  public Integer get_purchaser_id() {
    return purchaser_id;
  }
  public void set_purchaser_id(Integer purchaser_id) {
    this.purchaser_id = purchaser_id;
  }
  public QueryResult with_purchaser_id(Integer purchaser_id) {
    this.purchaser_id = purchaser_id;
    return this;
  }
  private Integer billing_record_id;
  public Integer get_billing_record_id() {
    return billing_record_id;
  }
  public void set_billing_record_id(Integer billing_record_id) {
    this.billing_record_id = billing_record_id;
  }
  public QueryResult with_billing_record_id(Integer billing_record_id) {
    this.billing_record_id = billing_record_id;
    return this;
  }
  private String cast__collection_deadline_at_aschar_;
  public String get_cast__collection_deadline_at_aschar_() {
    return cast__collection_deadline_at_aschar_;
  }
  public void set_cast__collection_deadline_at_aschar_(String cast__collection_deadline_at_aschar_) {
    this.cast__collection_deadline_at_aschar_ = cast__collection_deadline_at_aschar_;
  }
  public QueryResult with_cast__collection_deadline_at_aschar_(String cast__collection_deadline_at_aschar_) {
    this.cast__collection_deadline_at_aschar_ = cast__collection_deadline_at_aschar_;
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
  private String LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___;
  public String get_LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___() {
    return LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___;
  }
  public void set_LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___(String LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___) {
    this.LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___ = LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___;
  }
  public QueryResult with_LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___(String LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___) {
    this.LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___ = LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___;
    return this;
  }
  private String cast__review_status_id_aschar_;
  public String get_cast__review_status_id_aschar_() {
    return cast__review_status_id_aschar_;
  }
  public void set_cast__review_status_id_aschar_(String cast__review_status_id_aschar_) {
    this.cast__review_status_id_aschar_ = cast__review_status_id_aschar_;
  }
  public QueryResult with_cast__review_status_id_aschar_(String cast__review_status_id_aschar_) {
    this.cast__review_status_id_aschar_ = cast__review_status_id_aschar_;
    return this;
  }
  private String LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___;
  public String get_LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___() {
    return LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___;
  }
  public void set_LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___(String LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___) {
    this.LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___ = LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___;
  }
  public QueryResult with_LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___(String LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___) {
    this.LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___ = LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___;
    return this;
  }
  private String support_id;
  public String get_support_id() {
    return support_id;
  }
  public void set_support_id(String support_id) {
    this.support_id = support_id;
  }
  public QueryResult with_support_id(String support_id) {
    this.support_id = support_id;
    return this;
  }
  private String country_code;
  public String get_country_code() {
    return country_code;
  }
  public void set_country_code(String country_code) {
    this.country_code = country_code;
  }
  public QueryResult with_country_code(String country_code) {
    this.country_code = country_code;
    return this;
  }
  private Integer locale_id;
  public Integer get_locale_id() {
    return locale_id;
  }
  public void set_locale_id(Integer locale_id) {
    this.locale_id = locale_id;
  }
  public QueryResult with_locale_id(Integer locale_id) {
    this.locale_id = locale_id;
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
    equal = equal && (this.cast__uid_aschar_ == null ? that.cast__uid_aschar_ == null : this.cast__uid_aschar_.equals(that.cast__uid_aschar_));
    equal = equal && (this.purchaser_id == null ? that.purchaser_id == null : this.purchaser_id.equals(that.purchaser_id));
    equal = equal && (this.billing_record_id == null ? that.billing_record_id == null : this.billing_record_id.equals(that.billing_record_id));
    equal = equal && (this.cast__collection_deadline_at_aschar_ == null ? that.cast__collection_deadline_at_aschar_ == null : this.cast__collection_deadline_at_aschar_.equals(that.cast__collection_deadline_at_aschar_));
    equal = equal && (this.cast__created_at_aschar_ == null ? that.cast__created_at_aschar_ == null : this.cast__created_at_aschar_.equals(that.cast__created_at_aschar_));
    equal = equal && (this.cast__updated_at_aschar_ == null ? that.cast__updated_at_aschar_ == null : this.cast__updated_at_aschar_.equals(that.cast__updated_at_aschar_));
    equal = equal && (this.LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___ == null ? that.LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___ == null : this.LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___.equals(that.LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___));
    equal = equal && (this.cast__review_status_id_aschar_ == null ? that.cast__review_status_id_aschar_ == null : this.cast__review_status_id_aschar_.equals(that.cast__review_status_id_aschar_));
    equal = equal && (this.LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___ == null ? that.LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___ == null : this.LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___.equals(that.LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___));
    equal = equal && (this.support_id == null ? that.support_id == null : this.support_id.equals(that.support_id));
    equal = equal && (this.country_code == null ? that.country_code == null : this.country_code.equals(that.country_code));
    equal = equal && (this.locale_id == null ? that.locale_id == null : this.locale_id.equals(that.locale_id));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof QueryResult)) {
      return false;
    }
    QueryResult that = (QueryResult) o;
    boolean equal = true;
    equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
    equal = equal && (this.cast__uid_aschar_ == null ? that.cast__uid_aschar_ == null : this.cast__uid_aschar_.equals(that.cast__uid_aschar_));
    equal = equal && (this.purchaser_id == null ? that.purchaser_id == null : this.purchaser_id.equals(that.purchaser_id));
    equal = equal && (this.billing_record_id == null ? that.billing_record_id == null : this.billing_record_id.equals(that.billing_record_id));
    equal = equal && (this.cast__collection_deadline_at_aschar_ == null ? that.cast__collection_deadline_at_aschar_ == null : this.cast__collection_deadline_at_aschar_.equals(that.cast__collection_deadline_at_aschar_));
    equal = equal && (this.cast__created_at_aschar_ == null ? that.cast__created_at_aschar_ == null : this.cast__created_at_aschar_.equals(that.cast__created_at_aschar_));
    equal = equal && (this.cast__updated_at_aschar_ == null ? that.cast__updated_at_aschar_ == null : this.cast__updated_at_aschar_.equals(that.cast__updated_at_aschar_));
    equal = equal && (this.LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___ == null ? that.LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___ == null : this.LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___.equals(that.LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___));
    equal = equal && (this.cast__review_status_id_aschar_ == null ? that.cast__review_status_id_aschar_ == null : this.cast__review_status_id_aschar_.equals(that.cast__review_status_id_aschar_));
    equal = equal && (this.LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___ == null ? that.LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___ == null : this.LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___.equals(that.LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___));
    equal = equal && (this.support_id == null ? that.support_id == null : this.support_id.equals(that.support_id));
    equal = equal && (this.country_code == null ? that.country_code == null : this.country_code.equals(that.country_code));
    equal = equal && (this.locale_id == null ? that.locale_id == null : this.locale_id.equals(that.locale_id));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.id = JdbcWritableBridge.readInteger(1, __dbResults);
    this.cast__uid_aschar_ = JdbcWritableBridge.readString(2, __dbResults);
    this.purchaser_id = JdbcWritableBridge.readInteger(3, __dbResults);
    this.billing_record_id = JdbcWritableBridge.readInteger(4, __dbResults);
    this.cast__collection_deadline_at_aschar_ = JdbcWritableBridge.readString(5, __dbResults);
    this.cast__created_at_aschar_ = JdbcWritableBridge.readString(6, __dbResults);
    this.cast__updated_at_aschar_ = JdbcWritableBridge.readString(7, __dbResults);
    this.LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___ = JdbcWritableBridge.readString(8, __dbResults);
    this.cast__review_status_id_aschar_ = JdbcWritableBridge.readString(9, __dbResults);
    this.LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___ = JdbcWritableBridge.readString(10, __dbResults);
    this.support_id = JdbcWritableBridge.readString(11, __dbResults);
    this.country_code = JdbcWritableBridge.readString(12, __dbResults);
    this.locale_id = JdbcWritableBridge.readInteger(13, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.id = JdbcWritableBridge.readInteger(1, __dbResults);
    this.cast__uid_aschar_ = JdbcWritableBridge.readString(2, __dbResults);
    this.purchaser_id = JdbcWritableBridge.readInteger(3, __dbResults);
    this.billing_record_id = JdbcWritableBridge.readInteger(4, __dbResults);
    this.cast__collection_deadline_at_aschar_ = JdbcWritableBridge.readString(5, __dbResults);
    this.cast__created_at_aschar_ = JdbcWritableBridge.readString(6, __dbResults);
    this.cast__updated_at_aschar_ = JdbcWritableBridge.readString(7, __dbResults);
    this.LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___ = JdbcWritableBridge.readString(8, __dbResults);
    this.cast__review_status_id_aschar_ = JdbcWritableBridge.readString(9, __dbResults);
    this.LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___ = JdbcWritableBridge.readString(10, __dbResults);
    this.support_id = JdbcWritableBridge.readString(11, __dbResults);
    this.country_code = JdbcWritableBridge.readString(12, __dbResults);
    this.locale_id = JdbcWritableBridge.readInteger(13, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void loadLargeObjects0(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeInteger(id, 1 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeString(cast__uid_aschar_, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(purchaser_id, 3 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(billing_record_id, 4 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeString(cast__collection_deadline_at_aschar_, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(cast__created_at_aschar_, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(cast__updated_at_aschar_, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(cast__review_status_id_aschar_, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(support_id, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(country_code, 12 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeInteger(locale_id, 13 + __off, 5, __dbStmt);
    return 13;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeInteger(id, 1 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeString(cast__uid_aschar_, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(purchaser_id, 3 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(billing_record_id, 4 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeString(cast__collection_deadline_at_aschar_, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(cast__created_at_aschar_, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(cast__updated_at_aschar_, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(cast__review_status_id_aschar_, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(support_id, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(country_code, 12 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeInteger(locale_id, 13 + __off, 5, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.id = null;
    } else {
    this.id = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.cast__uid_aschar_ = null;
    } else {
    this.cast__uid_aschar_ = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.purchaser_id = null;
    } else {
    this.purchaser_id = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.billing_record_id = null;
    } else {
    this.billing_record_id = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.cast__collection_deadline_at_aschar_ = null;
    } else {
    this.cast__collection_deadline_at_aschar_ = Text.readString(__dataIn);
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
        this.LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___ = null;
    } else {
    this.LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___ = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.cast__review_status_id_aschar_ = null;
    } else {
    this.cast__review_status_id_aschar_ = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___ = null;
    } else {
    this.LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___ = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.support_id = null;
    } else {
    this.support_id = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.country_code = null;
    } else {
    this.country_code = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.locale_id = null;
    } else {
    this.locale_id = Integer.valueOf(__dataIn.readInt());
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.id);
    }
    if (null == this.cast__uid_aschar_) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, cast__uid_aschar_);
    }
    if (null == this.purchaser_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.purchaser_id);
    }
    if (null == this.billing_record_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.billing_record_id);
    }
    if (null == this.cast__collection_deadline_at_aschar_) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, cast__collection_deadline_at_aschar_);
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
    if (null == this.LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___);
    }
    if (null == this.cast__review_status_id_aschar_) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, cast__review_status_id_aschar_);
    }
    if (null == this.LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___);
    }
    if (null == this.support_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, support_id);
    }
    if (null == this.country_code) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, country_code);
    }
    if (null == this.locale_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.locale_id);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.id);
    }
    if (null == this.cast__uid_aschar_) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, cast__uid_aschar_);
    }
    if (null == this.purchaser_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.purchaser_id);
    }
    if (null == this.billing_record_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.billing_record_id);
    }
    if (null == this.cast__collection_deadline_at_aschar_) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, cast__collection_deadline_at_aschar_);
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
    if (null == this.LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___);
    }
    if (null == this.cast__review_status_id_aschar_) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, cast__review_status_id_aschar_);
    }
    if (null == this.LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___);
    }
    if (null == this.support_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, support_id);
    }
    if (null == this.country_code) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, country_code);
    }
    if (null == this.locale_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.locale_id);
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
    __sb.append(FieldFormatter.hiveStringReplaceDelims(cast__uid_aschar_==null?"":cast__uid_aschar_, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(purchaser_id==null?"":"" + purchaser_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(billing_record_id==null?"":"" + billing_record_id, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(cast__collection_deadline_at_aschar_==null?"":cast__collection_deadline_at_aschar_, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(cast__created_at_aschar_==null?"":cast__created_at_aschar_, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(cast__updated_at_aschar_==null?"":cast__updated_at_aschar_, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___==null?"":LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(cast__review_status_id_aschar_==null?"":cast__review_status_id_aschar_, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___==null?"":LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(support_id==null?"":support_id, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(country_code==null?"":country_code, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(locale_id==null?"":"" + locale_id, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(id==null?"":"" + id, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(cast__uid_aschar_==null?"":cast__uid_aschar_, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(purchaser_id==null?"":"" + purchaser_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(billing_record_id==null?"":"" + billing_record_id, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(cast__collection_deadline_at_aschar_==null?"":cast__collection_deadline_at_aschar_, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(cast__created_at_aschar_==null?"":cast__created_at_aschar_, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(cast__updated_at_aschar_==null?"":cast__updated_at_aschar_, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___==null?"":LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(cast__review_status_id_aschar_==null?"":cast__review_status_id_aschar_, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___==null?"":LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(support_id==null?"":support_id, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(country_code==null?"":country_code, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(locale_id==null?"":"" + locale_id, delimiters));
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
    if (__cur_str.equals("null")) { this.cast__uid_aschar_ = null; } else {
      this.cast__uid_aschar_ = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.purchaser_id = null; } else {
      this.purchaser_id = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.billing_record_id = null; } else {
      this.billing_record_id = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.cast__collection_deadline_at_aschar_ = null; } else {
      this.cast__collection_deadline_at_aschar_ = __cur_str;
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
    if (__cur_str.equals("null")) { this.LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___ = null; } else {
      this.LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___ = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.cast__review_status_id_aschar_ = null; } else {
      this.cast__review_status_id_aschar_ = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___ = null; } else {
      this.LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___ = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.support_id = null; } else {
      this.support_id = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.country_code = null; } else {
      this.country_code = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.locale_id = null; } else {
      this.locale_id = Integer.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.id = null; } else {
      this.id = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.cast__uid_aschar_ = null; } else {
      this.cast__uid_aschar_ = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.purchaser_id = null; } else {
      this.purchaser_id = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.billing_record_id = null; } else {
      this.billing_record_id = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.cast__collection_deadline_at_aschar_ = null; } else {
      this.cast__collection_deadline_at_aschar_ = __cur_str;
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
    if (__cur_str.equals("null")) { this.LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___ = null; } else {
      this.LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___ = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.cast__review_status_id_aschar_ = null; } else {
      this.cast__review_status_id_aschar_ = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___ = null; } else {
      this.LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___ = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.support_id = null; } else {
      this.support_id = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.country_code = null; } else {
      this.country_code = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.locale_id = null; } else {
      this.locale_id = Integer.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    QueryResult o = (QueryResult) super.clone();
    return o;
  }

  public void clone0(QueryResult o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new TreeMap<String, Object>();
    __sqoop$field_map.put("id", this.id);
    __sqoop$field_map.put("cast__uid_aschar_", this.cast__uid_aschar_);
    __sqoop$field_map.put("purchaser_id", this.purchaser_id);
    __sqoop$field_map.put("billing_record_id", this.billing_record_id);
    __sqoop$field_map.put("cast__collection_deadline_at_aschar_", this.cast__collection_deadline_at_aschar_);
    __sqoop$field_map.put("cast__created_at_aschar_", this.cast__created_at_aschar_);
    __sqoop$field_map.put("cast__updated_at_aschar_", this.cast__updated_at_aschar_);
    __sqoop$field_map.put("LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___", this.LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___);
    __sqoop$field_map.put("cast__review_status_id_aschar_", this.cast__review_status_id_aschar_);
    __sqoop$field_map.put("LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___", this.LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___);
    __sqoop$field_map.put("support_id", this.support_id);
    __sqoop$field_map.put("country_code", this.country_code);
    __sqoop$field_map.put("locale_id", this.locale_id);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("id", this.id);
    __sqoop$field_map.put("cast__uid_aschar_", this.cast__uid_aschar_);
    __sqoop$field_map.put("purchaser_id", this.purchaser_id);
    __sqoop$field_map.put("billing_record_id", this.billing_record_id);
    __sqoop$field_map.put("cast__collection_deadline_at_aschar_", this.cast__collection_deadline_at_aschar_);
    __sqoop$field_map.put("cast__created_at_aschar_", this.cast__created_at_aschar_);
    __sqoop$field_map.put("cast__updated_at_aschar_", this.cast__updated_at_aschar_);
    __sqoop$field_map.put("LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___", this.LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___);
    __sqoop$field_map.put("cast__review_status_id_aschar_", this.cast__review_status_id_aschar_);
    __sqoop$field_map.put("LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___", this.LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___);
    __sqoop$field_map.put("support_id", this.support_id);
    __sqoop$field_map.put("country_code", this.country_code);
    __sqoop$field_map.put("locale_id", this.locale_id);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if ("id".equals(__fieldName)) {
      this.id = (Integer) __fieldVal;
    }
    else    if ("cast__uid_aschar_".equals(__fieldName)) {
      this.cast__uid_aschar_ = (String) __fieldVal;
    }
    else    if ("purchaser_id".equals(__fieldName)) {
      this.purchaser_id = (Integer) __fieldVal;
    }
    else    if ("billing_record_id".equals(__fieldName)) {
      this.billing_record_id = (Integer) __fieldVal;
    }
    else    if ("cast__collection_deadline_at_aschar_".equals(__fieldName)) {
      this.cast__collection_deadline_at_aschar_ = (String) __fieldVal;
    }
    else    if ("cast__created_at_aschar_".equals(__fieldName)) {
      this.cast__created_at_aschar_ = (String) __fieldVal;
    }
    else    if ("cast__updated_at_aschar_".equals(__fieldName)) {
      this.cast__updated_at_aschar_ = (String) __fieldVal;
    }
    else    if ("LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___".equals(__fieldName)) {
      this.LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___ = (String) __fieldVal;
    }
    else    if ("cast__review_status_id_aschar_".equals(__fieldName)) {
      this.cast__review_status_id_aschar_ = (String) __fieldVal;
    }
    else    if ("LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___".equals(__fieldName)) {
      this.LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___ = (String) __fieldVal;
    }
    else    if ("support_id".equals(__fieldName)) {
      this.support_id = (String) __fieldVal;
    }
    else    if ("country_code".equals(__fieldName)) {
      this.country_code = (String) __fieldVal;
    }
    else    if ("locale_id".equals(__fieldName)) {
      this.locale_id = (Integer) __fieldVal;
    }
    else {
      throw new RuntimeException("No such field: " + __fieldName);
    }
  }
  public boolean setField0(String __fieldName, Object __fieldVal) {
    if ("id".equals(__fieldName)) {
      this.id = (Integer) __fieldVal;
      return true;
    }
    else    if ("cast__uid_aschar_".equals(__fieldName)) {
      this.cast__uid_aschar_ = (String) __fieldVal;
      return true;
    }
    else    if ("purchaser_id".equals(__fieldName)) {
      this.purchaser_id = (Integer) __fieldVal;
      return true;
    }
    else    if ("billing_record_id".equals(__fieldName)) {
      this.billing_record_id = (Integer) __fieldVal;
      return true;
    }
    else    if ("cast__collection_deadline_at_aschar_".equals(__fieldName)) {
      this.cast__collection_deadline_at_aschar_ = (String) __fieldVal;
      return true;
    }
    else    if ("cast__created_at_aschar_".equals(__fieldName)) {
      this.cast__created_at_aschar_ = (String) __fieldVal;
      return true;
    }
    else    if ("cast__updated_at_aschar_".equals(__fieldName)) {
      this.cast__updated_at_aschar_ = (String) __fieldVal;
      return true;
    }
    else    if ("LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___".equals(__fieldName)) {
      this.LOWER_CONCAT_LEFT_HEX__uuid___8______MID_HEX__uuid___9_4______MID_HEX__uuid___13_4______MID_HEX__uuid___17_4______RIGHT_HEX__uuid___12___ = (String) __fieldVal;
      return true;
    }
    else    if ("cast__review_status_id_aschar_".equals(__fieldName)) {
      this.cast__review_status_id_aschar_ = (String) __fieldVal;
      return true;
    }
    else    if ("LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___".equals(__fieldName)) {
      this.LOWER_CONCAT_LEFT_HEX__consumer_id___8______MID_HEX__consumer_id___9_4______MID_HEX__consumer_id___13_4______MID_HEX__consumer_id___17_4______RIGHT_HEX__consumer_id___12___ = (String) __fieldVal;
      return true;
    }
    else    if ("support_id".equals(__fieldName)) {
      this.support_id = (String) __fieldVal;
      return true;
    }
    else    if ("country_code".equals(__fieldName)) {
      this.country_code = (String) __fieldVal;
      return true;
    }
    else    if ("locale_id".equals(__fieldName)) {
      this.locale_id = (Integer) __fieldVal;
      return true;
    }
    else {
      return false;    }
  }
}
