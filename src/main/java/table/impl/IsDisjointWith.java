package table.impl;

import com.datastax.oss.driver.api.core.type.DataTypes;
import database.Database;
import table.CassandraTable;

import java.io.Serializable;
import java.text.MessageFormat;

public class IsDisjointWith extends CassandraTable {

    public IsDisjointWith(Database database) {
        super("IsDisjointWith", database);
        this.OntologyIndex = 0;

        this.PartitionKeyColumns.put("cls", DataTypes.TEXT);
        this.ClusteringKeyColumns.put("num", DataTypes.INT);

        this.Columns.put("ind1", DataTypes.TEXT);
    }

    public static class Row implements Serializable {

        private final String cls;
        private final Integer num;
        private final String ind1;

        public Row(String cls, Integer num, String ind1) {
            this.cls = cls;
            this.num = num;
            this.ind1 = ind1;
        }

        public String getCls() {
            return cls;
        }

        public Integer getNum() {
            return num;
        }

        public String getInd1() {
            return ind1;
        }

        @Override
        public String toString() {
            return MessageFormat.format("IsDisjointWith'{'cls={0}, num=''{1}'', ind1={2}'}'", cls, num, ind1);
        }
    }

}
