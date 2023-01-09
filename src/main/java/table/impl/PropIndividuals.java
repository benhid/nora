package table.impl;

import com.datastax.oss.driver.api.core.type.DataTypes;
import database.Database;
import table.CassandraTable;

import java.io.Serializable;
import java.text.MessageFormat;

public class PropIndividuals extends CassandraTable {

    public PropIndividuals(Database database) {
        super("PropIndividuals", database);
        this.OntologyIndex = 0;

        this.PartitionKeyColumns.put("prop", DataTypes.TEXT);
        this.ClusteringKeyColumns.put("num", DataTypes.INT);

        this.Columns.put("domain", DataTypes.TEXT);
        this.Columns.put("range", DataTypes.TEXT);
    }

    public static class Row implements Serializable {

        private final String prop;
        private final String domain;
        private final String range;

        public Row(String domain, String range, String prop) {
            this.prop = prop;
            this.domain = domain;
            this.range = range;
        }

        public String getProp() {
            return prop;
        }

        public String getDomain() {
            return domain;
        }

        public String getRange() {
            return range;
        }

        @Override
        public String toString() {
            return MessageFormat.format("PropIndividuals'{'prop={0}, domain={1}, range={2},'}'", prop, domain, range);
        }

    }

}
