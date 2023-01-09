package table.impl;

import com.datastax.oss.driver.api.core.type.DataTypes;
import database.Database;
import table.CassandraTable;

import java.io.Serializable;
import java.text.MessageFormat;

public class ClassIndividuals extends CassandraTable {

    public ClassIndividuals(Database database) {
        super("ClassIndividuals", database);
        this.OntologyIndex = 0;

        this.PartitionKeyColumns.put("cls", DataTypes.TEXT);
        this.ClusteringKeyColumns.put("num", DataTypes.INT);

        this.Columns.put("individual", DataTypes.TEXT);
    }

    public static class Row implements Serializable {

        private final String cls;
        private final Integer num;
        private final String individual;

        public Row(String cls, String individual) {
            this.cls = cls;
            this.num = 1;
            this.individual = individual;
        }

        public Row(String cls, Integer num, String individual) {
            this.cls = cls;
            this.num = num;
            this.individual = individual;
        }

        public String getCls() {
            return cls;
        }

        public Integer getNum() {
            return num;
        }

        public String getIndividual() {
            return individual;
        }

        @Override
        public String toString() {
            return MessageFormat.format("ClassIndividuals'{'cls={0}, num=''{1}'', individual={2}'}'", cls, num, individual);
        }

    }

}
