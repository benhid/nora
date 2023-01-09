package table.impl;

import com.datastax.oss.driver.api.core.type.DataTypes;
import database.Database;
import table.CassandraTable;

import java.io.Serializable;
import java.text.MessageFormat;

public class IsObjectPropertyInverseOf extends CassandraTable {

    public IsObjectPropertyInverseOf(Database database) {
        super("IsInverseOf", database);
        this.OntologyIndex = 0;

        this.PartitionKeyColumns.put("prop", DataTypes.TEXT);
        this.ClusteringKeyColumns.put("num", DataTypes.INT);

        this.Columns.put("inverse", DataTypes.TEXT);
    }

    public static class Row implements Serializable {

        private final String prop;
        private final Integer num;
        private final String inverse;

        public Row(String prop, Integer num, String inverse) {
            this.prop = prop;
            this.num = num;
            this.inverse = inverse;
        }

        public String getProp() {
            return prop;
        }

        public Integer getNum() {
            return num;
        }

        public String getInverse() {
            return inverse;
        }

        @Override
        public String toString() {
            return MessageFormat.format("IsInverseOf'{'prop={0}, num=''{1}'', inverse={2}'}'", prop, num, inverse);
        }

    }

}
