package table.impl;

import com.datastax.oss.driver.api.core.type.DataTypes;
import database.Database;
import table.CassandraTable;

import java.io.Serializable;
import java.text.MessageFormat;

public class IsSubclassOfClass extends CassandraTable {

    public IsSubclassOfClass(Database database) {
        super("IsSubclassOfClass", database);
        this.OntologyIndex = 0;

        this.PartitionKeyColumns.put("cls", DataTypes.TEXT);
        this.ClusteringKeyColumns.put("num", DataTypes.INT);

        this.Columns.put("supclass", DataTypes.TEXT);
    }

    public static class Row implements Serializable {

        private final String cls;
        private final Integer num;
        private final String supclass;

        public Row(String cls, Integer num, String supclass) {
            this.cls = cls;
            this.num = num;
            this.supclass = supclass;
        }

        public String getCls() {
            return cls;
        }

        public Integer getNum() {
            return num;
        }

        public String getSupclass() {
            return supclass;
        }

        @Override
        public String toString() {
            return MessageFormat.format("IsSubclassOfClass'{'cls={0}, num=''{1}'', supclass={2}'}'", cls, num, supclass);
        }
    }

}
