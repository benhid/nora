package table.impl;

import com.datastax.oss.driver.api.core.type.DataTypes;
import database.Database;
import table.CassandraTable;

import java.io.Serializable;
import java.text.MessageFormat;

public class IsInverseFunctionalProperty extends CassandraTable {

    public IsInverseFunctionalProperty(Database database) {
        super("IsInverseFunctionalProperty", database);
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
            return MessageFormat.format("IsInverseFunctionalProperty'{'prop={0}'}'", prop);
        }

    }
}
