/**
 * /var/groupon/tung/tungsten/tungsten-replicator/samples/extensions/javascript/fixmysqlstrings.js
 * VMware Continuent Tungsten Replicator
 * Copyright (C) 2015 VMware, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This filter fixes MySQL strings by converting byte values to either a
 * normal Java String or a Hex'ed string if the source type is VARBINARY
 * or BINARY.  After fixing strings it marks the strings=utf8 option so
 * that downstream filters can see strings have been fixed.  It also
 * removes the ##charset tag from row events as this is no longer needed
 * after strings are converted.
 *
 * The filter is designed to be used in place of the MySQL option
 * usingBytesForStrings=true, which cannot be used in cases where the
 * master replicator is also generating log records to be applied to other
 * MySQL servers.
 *
 * IMPORTANT: For this script to work you must run the colnames filter
 * to fill in the type description.  It can run anywhere upstream as the
 * value is now preserved in the log. The tpm --enable-heterogeneous-master
 * option will cause colnames to be applied and is the recommended method
 * to enable metadata collection.
 *
 * @author <a href="mailto:eric.stone@continuent.com">Eric M. Stone</a>
 * @author <a href="mailto:mc.brown@vmware.com">MC Brown</a>
 * @author <a href="mailto:robert.hodges@continuent.com">Robert M. Hodges</a>
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 */

/**
 * Called once when JavaScriptFilter corresponding to this script is prepared.
 */

var fieldtypes = [];

function prepare() {
  logger.info("FMS: fixMysqlStrings: Initializing...");
    fieldlist = new String(filterProperties.getString("fieldtypes"));
    fieldbase = fieldlist.split(',');

    for (i=0;i<fieldbase.length;i++) {
	field = new String(fieldbase[i]);
	fieldtypes[field] = 1;
    }
}

/**
 * Called on every filtered event. See replicator's javadoc for more details
 * on accessible classes. Also, JavaScriptFilter's javadoc contains description
 * about how to define a script like this.
 *
 * @param event Filtered com.continuent.tungsten.replicator.event.ReplDBMSEvent
 *
 * @see com.continuent.tungsten.replicator.filter.JavaScriptFilter
 * @see com.continuent.tungsten.replicator.event.ReplDBMSEvent
 * @see com.continuent.tungsten.replicator.dbms.DBMSData
 * @see com.continuent.tungsten.replicator.dbms.StatementData
 * @see com.continuent.tungsten.replicator.dbms.RowChangeData
 * @see com.continuent.tungsten.replicator.dbms.OneRowChange
 * @see com.continuent.tungsten.replicator.dbms.RowChangeData.ActionType
 * @see com.continuent.tungsten.replicator.thl.THLManagerCtrl#printRowChangeData(StringBuilder, RowChangeData, String, boolean, int)
 * @see java.lang.Thread
 * @see org.apache.log4j.Logger
 */
function filter(event) {
  // Ensure that we are dealing with a MySQL event. If not, we can stop.
  //  logger.info("FMS: fixMysqlStrings: Processing event");

  raw_event = event.getDBMSEvent();
  if (raw_event == null)
  {
      logger.info("FMS: Event was null!");
    return;
  }
  dbms_type = raw_event.getMetadataOptionValue("dbms_type");
  if (dbms_type != "mysql")
  {
    logger.info("FMS: Event was not mysql: [" + dbms_type + "]");
    return;
  }

  // Get the data.
  data = event.getData();
  if (data != null) {
    // One ReplDBMSEvent may contain many DBMSData events.
    for (i = 0; i < data.size(); i++) {
      // Get com.continuent.tungsten.replicator.dbms.DBMSData
      d = data.get(i);

      // Determine the underlying type of DBMSData event.
      if (d != null && d instanceof com.continuent.tungsten.replicator.dbms.StatementData) {
        // Convert statement data from bytes to string.
        query = d.getQuery();
        d.setQuery(query);
      } else if (d != null && d instanceof com.continuent.tungsten.replicator.dbms.RowChangeData) {
        processRowChanges(event, d);
      }
    }
  }

  // If we made it this far, byte values are properly translated to UTF8.
  // Make a note to that effect.
  raw_event.setMetaDataOption("strings", "utf8");
}

// Convert String bytes to blob or string based on type description.
function processRowChanges(event, d) {
  rowChanges = d.getRowChanges();
  // logger.info("FMS: fixMysqlStrings: Processing rowchanges");

  // Find the applicable charset for converting strings, if known.
  charsetName = d.getOption("##charset");
  if (charsetName == null)
    charset = null;
  else
    charset = java.nio.charset.Charset.forName(charsetName);

  // One RowChangeData may contain many OneRowChange events.
  for (j = 0; j < rowChanges.size(); j++) {
    // Get com.continuent.tungsten.replicator.dbms.OneRowChange
    oneRowChange = rowChanges.get(j);
    var schema = oneRowChange.getSchemaName();
    var table = oneRowChange.getTableName();
    var columns = oneRowChange.getColumnSpec();
    var columnValues = oneRowChange.getColumnValues();
    fixUpStrings(schema, table, columns, columnValues, charset);

    // Iterate through its keys if any
    keys = oneRowChange.getKeySpec();
    keyValues = oneRowChange.getKeyValues();
    fixUpStrings(schema, table, keys, keyValues, charset);
  }

  // If the character set is set, try to remove the option from the
  // row change event, as it is no longer needed now that strings are
  // converted.
  if (charsetName != null)
    d.removeOption("##charset");
}

// Look for strings to fix up.
function fixUpStrings(schema, table, columns, columnValues, charset)
{
  // logger.info("FMS: fix: Processing an entry");
  for (c = 0; c < columns.size(); c++) {
    columnSpec = columns.get(c);
    colName = columnSpec.getName();
    colType = columnSpec.getType();
    colDesc = columnSpec.getTypeDescription();

      baseType = new String(colDesc).replace(/\([0-9]+\)/g,"");

    // See if we have a string that needs sorting out.  12 is VARCHAR
    // from java.sql.Type.VARCHAR.

    if (colType == 12) {
      logger.debug("Found a VARCHAR column that may need sorting: column=" + colName + ' table=' + schema + '.' + table);
      // Iterate through the rows.
      for (row = 0; row < columnValues.size(); row++) {
        // Ensure values are actually there--for insert keys they may not be.
        values = columnValues.get(row);
        if (row >= values.size())
          break;

	  // Fetch the values.
        value = values.get(c);
        raw_v = value.getValue();
        if (raw_v == null || colDesc == null) {
          // Do nothing; we have a null value or are missing column data.
          logger.debug('value: NULL');
        }
	  else if (raw_v instanceof java.lang.String) {
          // It's already a string, so we cannot convert it...
          logger.debug('value: ' + raw_v);
        } else {
            if (charset == null)
		value.setValue(new java.lang.String(raw_v));
            else
              value.setValue(new java.lang.String(raw_v, charset));
          }
        }
      }

      // Now check if we have to do an explicit hex conversion

    if (baseType in fieldtypes) {
	// logger.info("FMS: Found a column that needs hexing: column=" + colName + ' table=' + schema + '.' + table);
	// Iterate through the rows.
	for (row = 0; row < columnValues.size(); row++) {
            // Ensure values are actually there--for insert keys they may not be.
            values = columnValues.get(row);
            if (row >= values.size())
		break;
	
            // Fetch the values.
            value = values.get(c);
	    raw_v = value.getValue();

            if (raw_v == null || colDesc == null) {
		// Do nothing; we have a null value or are missing column data.
		logger.info('value is NULL skipping');
            }
	    else {
		// logger.info("FMS: We should do something... with " + baseType);
		// logger.info("FMS: Converting a " + baseType + " value to hex");
		// Convert to a hexadecimal string.

                if (value.getValue() instanceof com.continuent.tungsten.replicator.extractor.mysql.SerialBlob)
                {
                    hex = javax.xml.bind.DatatypeConverter.printHexBinary(raw_v.getBytes(1,raw_v.length()));
		    // logger.info("Seialblob is converted from " + raw_v + " to " + hex);
                } else { 
		    if (charset == null)
			hex = javax.xml.bind.DatatypeConverter.printHexBinary(raw_v.getBytes());
		    else
			hex = javax.xml.bind.DatatypeConverter.printHexBinary(raw_v.getBytes(charset));
                }

		// logger.info(hex);

		if (colDesc.startsWith("BINARY")) {
		    // Conversion cuts off trailing x'00' bytes in BINARY strings.
		    // We compute the proper length from the type name and append
		    // the missing null values.
		    re = /BINARY\(([0-9]+)\)/;
		    match = re.exec(colDesc);
		    expectLen = match[1] * 2;
		    actLen = hex.length();
		    diff = expectLen - actLen;
		    if (diff > 0)
		    {
			extraZeroes = "";
			for (zeroes = 0; zeroes < diff; zeroes++) {
			    extraZeroes += "0";
			}
			hex += extraZeroes;
		    }
		}

                if (value.getValue() instanceof com.continuent.tungsten.replicator.extractor.mysql.SerialBlob)
                {
                    var blob = com.continuent.tungsten.replicator.extractor.mysql.SerialBlob(hex.getBytes());
                    value.setValue(blob);
                } else {
		    value.setValue(hex);
                }
	    }
	}
    }
  }
}
