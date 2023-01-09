package table.impl;

import com.datastax.oss.driver.api.core.type.DataTypes;
import database.Database;
import table.CassandraTable;

import java.io.Serializable;
import java.text.MessageFormat;

public class IsObjectPropertyDomain extends CassandraTable {

    public IsObjectPropertyDomain(Database database) {
        super("IsOPDomain", database);
        this.OntologyIndex = 0;

        this.PartitionKeyColumns.put("domain", DataTypes.TEXT);
        this.ClusteringKeyColumns.put("num", DataTypes.INT);

        this.Columns.put("property", DataTypes.TEXT);
    }

    public static class Row implements Serializable {

        private final String domain;
        private final Integer num;
        private final String property;

        public Row(String domain, Integer num, String property) {
            this.domain = domain;
            this.num = num;
            this.property = property;
        }

        public String getDomain() {
            return domain;
        }

        public Integer getNum() {
            return num;
        }

        public String getProperty() {
            return property;
        }

        @Override
        public String toString() {
            return MessageFormat.format("IsObjectPropertyDomain'{'domain={0}, num=''{1}'', property={1}'}'", domain, num, property);
        }

    }


}
