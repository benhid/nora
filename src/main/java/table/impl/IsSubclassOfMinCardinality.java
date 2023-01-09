package table.impl;

import com.datastax.oss.driver.api.core.type.DataTypes;
import database.Database;
import table.CassandraTable;

import java.io.Serializable;
import java.text.MessageFormat;

public class IsSubclassOfMinCardinality extends CassandraTable {

    public IsSubclassOfMinCardinality(Database database) {
        super("IsSubclassOfMinCardinality", database);
        this.OntologyIndex = 0;

        this.PartitionKeyColumns.put("cls", DataTypes.TEXT);
        this.ClusteringKeyColumns.put("num", DataTypes.INT);

        this.Columns.put("prop", DataTypes.TEXT);
        this.Columns.put("card", DataTypes.TEXT);
        this.Columns.put("clss", DataTypes.TEXT);
    }

    public static class Row implements Serializable {

        private final String cls;
        private final Integer num;
        private final String prop;
        private final String card;
        private final String clss;

        public Row(String cls, Integer num, String prop, String card, String clss) {
            this.cls = cls;
            this.num = num;
            this.prop = prop;
            this.card = card;
            this.clss = clss;
        }

        public String getCls() {
            return cls;
        }

        public Integer getNum() {
            return num;
        }

        public String getProp() {
            return prop;
        }

        public String getCard() {
            return card;
        }

        public String getClss() {
            return clss;
        }

        @Override
        public String toString() {
            return MessageFormat.format("IsSubclassOfMinCardinality'{'cls={0}, num=''{1}'', prop={2}, card={3}, clss={4}'}'", cls, num, prop, card, clss);
        }

    }

}
