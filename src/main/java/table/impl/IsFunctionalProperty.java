package table.impl;

import com.datastax.oss.driver.api.core.type.DataTypes;
import database.Database;
import table.CassandraTable;

import java.io.Serializable;
import java.text.MessageFormat;

public class IsFunctionalProperty extends CassandraTable {

    public IsFunctionalProperty(Database database) {
        super("IsFunctionalProperty", database);
        this.OntologyIndex = 0;

        this.PartitionKeyColumns.put("prop", DataTypes.TEXT);
    }

    public static class Row implements Serializable {

        private final String prop;

        public Row(String prop) {
            this.prop = prop;
        }

        public String getProp() {
            return prop;
        }

        @Override
        public String toString() {
            return MessageFormat.format("IsFunctionalProperty'{'prop={0}'}'", prop);
        }

    }

}
