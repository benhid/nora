package table.impl;

import com.datastax.oss.driver.api.core.type.DataTypes;
import database.Database;
import table.CassandraTable;

import java.io.Serializable;
import java.text.MessageFormat;

public class IsSubPropertyOf extends CassandraTable {

    public IsSubPropertyOf(Database database) {
        super("IsSubpropertyOf", database);
        this.OntologyIndex = 0;

        this.PartitionKeyColumns.put("prop", DataTypes.TEXT);
        this.ClusteringKeyColumns.put("num", DataTypes.INT);

        this.Columns.put("subprop", DataTypes.TEXT);
    }

    public static class Row implements Serializable {

        private final String prop;
        private final Integer num;
        private final String subprop;

        public Row(String prop, Integer num, String subprop) {
            this.prop = prop;
            this.num = num;
            this.subprop = subprop;
        }

        public String getProp() {
            return prop;
        }

        public Integer getNum() {
            return num;
        }

        public String getSubprop() {
            return subprop;
        }

        @Override
        public String toString() {
            return MessageFormat.format("IsSubpropertyOf'{'prop={0}, num=''{1}'', subprop={2}'}'", prop, num, subprop);
        }
    }

}
