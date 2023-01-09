package table.impl;

import com.datastax.oss.driver.api.core.type.DataTypes;
import database.Database;
import table.CassandraTable;

import java.io.Serializable;
import java.text.MessageFormat;

public class IsEquivalentToClass extends CassandraTable {

    public IsEquivalentToClass(Database database) {
        super("IsEquivalentToClass", database);
        this.OntologyIndex = 0;

        this.PartitionKeyColumns.put("cls", DataTypes.TEXT);
        this.ClusteringKeyColumns.put("num", DataTypes.INT);

        this.Columns.put("equiv", DataTypes.TEXT);
    }

    public static class Row implements Serializable {

        private final String cls;
        private final Integer num;
        private final String equiv;

        public Row(String cls, Integer num, String equiv) {
            this.cls = cls;
            this.num = num;
            this.equiv = equiv;
        }

        public String getCls() {
            return cls;
        }

        public Integer getNum() {
            return num;
        }

        public String getEquiv() {
            return equiv;
        }

        @Override
        public String toString() {
            return MessageFormat.format("IsEquivalentToClass'{'cls={0}, num=''{1}'', equiv={2}'}'", cls, num, equiv);
        }

    }
}
