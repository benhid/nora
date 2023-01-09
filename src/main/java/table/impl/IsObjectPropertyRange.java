package table.impl;

import com.datastax.oss.driver.api.core.type.DataTypes;
import database.Database;
import table.CassandraTable;

import java.io.Serializable;
import java.text.MessageFormat;

public class IsObjectPropertyRange extends CassandraTable {

    public IsObjectPropertyRange(Database database) {
        super("IsOPRange", database);
        this.OntologyIndex = 0;

        this.PartitionKeyColumns.put("range", DataTypes.TEXT);
        this.ClusteringKeyColumns.put("num", DataTypes.INT);

        this.Columns.put("property", DataTypes.TEXT);
    }

    public static class Row implements Serializable {

        private final String range;
        private final Integer num;
        private final String property;

        public Row(String range, Integer num, String property) {
            this.range = range;
            this.num = num;
            this.property = property;
        }

        public String getRange() {
            return range;
        }

        public Integer getNum() {
            return num;
        }

        public String getProperty() {
            return property;
        }

        @Override
        public String toString() {
            return MessageFormat.format("IsObjectPropertyRange'{'range={0}, num=''{1}'', property={1}'}'", range, num, property);
        }

    }

}
