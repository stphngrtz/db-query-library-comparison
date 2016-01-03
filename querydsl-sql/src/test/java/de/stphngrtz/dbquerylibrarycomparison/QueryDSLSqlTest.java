package de.stphngrtz.dbquerylibrarycomparison;

import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.sql.Configuration;
import com.querydsl.sql.PostgreSQLTemplates;
import com.querydsl.sql.SQLExpressions;
import com.querydsl.sql.SQLQueryFactory;
import org.junit.BeforeClass;
import org.junit.Test;
import org.postgresql.ds.PGPoolingDataSource;

import java.util.Objects;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class QueryDSLSqlTest {

    private static final String db_url = "192.168.99.100";
    private static final String db_port = "5432";
    private static final String db_username = "stephan";
    private static final String db_password = "mysecretpassword";
    private static final String db_database = db_username;

    private static SQLQueryFactory queryFactory;

    @BeforeClass
    public static void setUp() throws Exception {
        PostgreSQLTemplates templates = new PostgreSQLTemplates();
        Configuration configuration = new Configuration(templates);

        PGPoolingDataSource pgPoolingDataSource = new PGPoolingDataSource();
        pgPoolingDataSource.setDataSourceName("A Data Source");
        pgPoolingDataSource.setServerName(db_url);
        pgPoolingDataSource.setPortNumber(Integer.valueOf(db_port));
        pgPoolingDataSource.setDatabaseName(db_database);
        pgPoolingDataSource.setUser(db_username);
        pgPoolingDataSource.setPassword(db_password);
        pgPoolingDataSource.setMaxConnections(10);

        queryFactory = new SQLQueryFactory(configuration, pgPoolingDataSource);
    }

    @Test
    public void selectAll() throws Exception {
        QUsers qUsers = new QUsers("users");
        assertThat(queryFactory
                        .select(qUsers.all())
                        .from(qUsers)
                        .fetch()
                        .stream().map(t -> new User(t.get(qUsers.id), t.get(qUsers.name), t.get(qUsers.email))).collect(Collectors.toList()),
                containsInAnyOrder(
                        new User(1, "Stephan", "stephan.goertz@gmail.com"),
                        new User(2, "Steffi", "steffi05.04@freenet.de"),
                        new User(3, "Willi", "willi@web.de"),
                        new User(4, "Franz", "franz@web.de")
                )
        );
    }

    @Test
    public void selectAllOrderedBy() throws Exception {
        QRoles qRoles = new QRoles("roles");
        assertThat(queryFactory
                        .select(qRoles.all())
                        .from(qRoles)
                        .orderBy(qRoles.name.desc())
                        .fetch()
                        .stream().map(t -> new Role(t.get(qRoles.id), t.get(qRoles.name))).collect(Collectors.toList()),
                contains(
                        new Role(2, "Developer"),
                        new Role(3, "Designer"),
                        new Role(1, "Admin")
                )
        );
    }

    @Test
    public void selectWhereEquals() throws Exception {
        QUsers qUsers = new QUsers("users");
        assertThat(queryFactory
                        .select(qUsers.all())
                        .from(qUsers)
                        .where(qUsers.id.eq(1))
                        .fetch()
                        .stream().map(t -> new User(t.get(qUsers.id), t.get(qUsers.name), t.get(qUsers.email))).collect(Collectors.toList()),
                contains(
                        new User(1, "Stephan", "stephan.goertz@gmail.com")
                )
        );
    }

    @Test
    public void selectWhereLike() throws Exception {
        QUsers qUsers = new QUsers("users");
        assertThat(queryFactory
                        .select(qUsers.all())
                        .from(qUsers)
                        .where(qUsers.email.like("%@gmail.com"))
                        .fetch()
                        .stream().map(t -> new User(t.get(qUsers.id), t.get(qUsers.name), t.get(qUsers.email))).collect(Collectors.toList()),
                contains(
                        new User(1, "Stephan", "stephan.goertz@gmail.com")
                )
        );
    }

    @Test
    public void selectWithConstant() throws Exception {
        QUsers qUsers = new QUsers("users");
        assertThat(queryFactory
                        .select(qUsers.id, qUsers.name, Expressions.constant("mail@me.de"))
                        .from(qUsers)
                        .where(qUsers.id.eq(1))
                        .fetch()
                        .stream().map(t -> new User(t.get(qUsers.id), t.get(qUsers.name), t.get(2, String.class))).collect(Collectors.toList()),
                contains(
                        new User(1, "Stephan", "mail@me.de")
                )
        );
    }

    @Test
    public void selectWithSubselectInSelectBlock() throws Exception {
        QUsers qUsers = new QUsers("users");
        QUsersWithRoles qUsersWithRoles = new QUsersWithRoles("usersWithRoles");
        assertThat(queryFactory
                        .select(qUsers.name, SQLExpressions.select(qUsersWithRoles.count()).from(qUsersWithRoles).where(qUsersWithRoles.userId.eq(qUsers.id)))
                        .from(qUsers)
                        .fetch()
                        .stream().map(t -> t.get(qUsers.name) + " (" + t.get(1, Integer.class) + ")").collect(Collectors.toList()),
                containsInAnyOrder(
                        "Stephan (2)",
                        "Steffi (1)",
                        "Willi (0)",
                        "Franz (1)"
                )
        );
    }

    @Test
    public void selectWithSubselectInWhereBlock() throws Exception {
        QUsers qUsers = new QUsers("users");
        QUsersWithRoles qUsersWithRoles = new QUsersWithRoles("usersWithRoles");
        assertThat(queryFactory
                        .select(qUsers.name)
                        .from(qUsers)
                        .where(qUsers.id.in(SQLExpressions.select(qUsersWithRoles.userId).from(qUsersWithRoles)))
                        .fetch(),
                containsInAnyOrder(
                        "Stephan",
                        "Steffi",
                        "Franz"
                )
        );
    }

    @Test
    @SuppressWarnings("unchecked")
    public void selectUnionSelect() throws Exception {
        QUsers qUsers1 = new QUsers("users1");
        QUsers qUsers2 = new QUsers("users2");
        assertThat(queryFactory.query()
                        .union(
                                SQLExpressions.select(qUsers1.id, qUsers1.name).from(qUsers1).where(qUsers1.id.eq(1)),
                                SQLExpressions.select(qUsers2.id, qUsers2.name).from(qUsers2).where(qUsers2.id.eq(2))
                        )
                        .fetch()
                        .stream().map(t -> new User(t.get(0, Integer.class), t.get(1, String.class), null)).collect(Collectors.toList()),
                contains(
                        new User(1, "Stephan", null),
                        new User(2, "Steffi", null)
                )
        );
    }

    @Test
    public void selectWithImplicitJoin() throws Exception {
        QUsers qUsers = new QUsers("users");
        QRoles qRoles = new QRoles("roles");
        QUsersWithRoles qUsersWithRoles = new QUsersWithRoles("usersWithRoles");

        assertThat(queryFactory
                        .select(qUsers.name, qRoles.name)
                        .from(qUsers, qRoles, qUsersWithRoles)
                        .where(
                                qUsers.id.eq(qUsersWithRoles.userId),
                                qRoles.id.eq(qUsersWithRoles.roleId)
                        )
                        .fetch()
                        .stream().map(t -> t.get(qUsers.name) + " ist " + t.get(qRoles.name)).collect(Collectors.toList()),
                containsInAnyOrder(
                        "Stephan ist Admin",
                        "Stephan ist Developer",
                        "Steffi ist Designer",
                        "Franz ist Developer"
                )
        );
    }

    @Test
    public void selectWithExplicitJoin() throws Exception {
        QUsers qUsers = new QUsers("users");
        QRoles qRoles = new QRoles("roles");
        QUsersWithRoles qUsersWithRoles = new QUsersWithRoles("usersWithRoles");

        assertThat(queryFactory
                        .select(qUsers.name, qRoles.name)
                        .from(qUsers)
                        .join(qUsersWithRoles).on(qUsersWithRoles.userId.eq(qUsers.id))
                        .join(qRoles).on(qRoles.id.eq(qUsersWithRoles.roleId))
                        .fetch()
                        .stream().map(t -> t.get(qUsers.name) + " ist " + t.get(qRoles.name)).collect(Collectors.toList()),
                containsInAnyOrder(
                        "Stephan ist Admin",
                        "Stephan ist Developer",
                        "Steffi ist Designer",
                        "Franz ist Developer"
                )
        );
    }

    @Test
    public void selectWithExplicitLeftOuterJoin() throws Exception {
        QUsers qUsers = new QUsers("users");
        QRoles qRoles = new QRoles("roles");
        QUsersWithRoles qUsersWithRoles = new QUsersWithRoles("usersWithRoles");

        assertThat(queryFactory
                        .select(qUsers.name, qRoles.name)
                        .from(qUsers)
                        .leftJoin(qUsersWithRoles).on(qUsersWithRoles.userId.eq(qUsers.id))
                        .leftJoin(qRoles).on(qRoles.id.eq(qUsersWithRoles.roleId))
                        .fetch()
                        .stream().map(t -> t.get(qUsers.name) + " ist " + t.get(qRoles.name)).collect(Collectors.toList()),
                containsInAnyOrder(
                        "Stephan ist Admin",
                        "Stephan ist Developer",
                        "Steffi ist Designer",
                        "Willi ist null",
                        "Franz ist Developer"
                )
        );
    }

    @Test
    public void selectCountWithGroupBy() throws Exception {
        QRoles qRoles = new QRoles("roles");
        QUsersWithRoles qUsersWithRoles = new QUsersWithRoles("usersWithRoles");
        assertThat(queryFactory
                        .select(qRoles.name, qUsersWithRoles.count())
                        .from(qRoles)
                        .join(qUsersWithRoles).on(qUsersWithRoles.roleId.eq(qRoles.id))
                        .groupBy(qRoles.name)
                        .fetch()
                        .stream().map(t -> t.get(qRoles.name) + " mit " + t.get(qUsersWithRoles.count()) + " User(n)").collect(Collectors.toList()),
                containsInAnyOrder(
                        "Admin mit 1 User(n)",
                        "Developer mit 2 User(n)",
                        "Designer mit 1 User(n)"
                )
        );
    }

    @Test
    public void selectCountWithGroupByHaving() throws Exception {
        QRoles qRoles = new QRoles("roles");
        QUsersWithRoles qUsersWithRoles = new QUsersWithRoles("usersWithRoles");
        assertThat(queryFactory
                        .select(qRoles.name, qUsersWithRoles.count())
                        .from(qRoles)
                        .join(qUsersWithRoles).on(qUsersWithRoles.roleId.eq(qRoles.id))
                        .groupBy(qRoles.name)
                        .having(qUsersWithRoles.count().gt(1))
                        .fetch()
                        .stream().map(t -> t.get(qRoles.name) + " mit " + t.get(qUsersWithRoles.count()) + " User(n)").collect(Collectors.toList()),
                contains(
                        "Developer mit 2 User(n)"
                )
        );
    }

    @Test
    public void window() throws Exception {
        // siehe http://blog.jooq.org/2013/11/03/probably-the-coolest-sql-feature-window-functions/
        QUsers qUsers = new QUsers("users");
        assertThat(queryFactory
                        .select(qUsers.name, SQLExpressions.left(qUsers.name, 1), SQLExpressions.count().over().partitionBy(SQLExpressions.left(qUsers.name, 1)))
                        .from(qUsers)
                        .orderBy(qUsers.name.asc())
                        .fetch()
                        .stream().map(t -> t.get(0, String.class) + " " + t.get(1, String.class) + " " + t.get(2, Integer.class)).collect(Collectors.toList()),
                contains(
                        "Franz F 1",
                        "Steffi S 2",
                        "Stephan S 2",
                        "Willi W 1"
                )
        );
    }

    @Test
    public void multipleWindows() throws Exception {
        // siehe http://blog.jooq.org/2013/11/03/probably-the-coolest-sql-feature-window-functions/
        QUsers qUsers = new QUsers("users");
        assertThat(queryFactory
                        .select(SQLExpressions.lag(qUsers.name).over().orderBy(qUsers.name.asc()), qUsers.name, SQLExpressions.lead(qUsers.name).over().orderBy(qUsers.name.asc()))
                        .from(qUsers)
                        .orderBy(qUsers.name.asc())
                        .fetch()
                        .stream().map(t -> t.get(0, String.class) + " <- " + t.get(1, String.class) + " -> " + t.get(2, String.class)).collect(Collectors.toList()),
                contains(
                        "null <- Franz -> Steffi",
                        "Franz <- Steffi -> Stephan",
                        "Steffi <- Stephan -> Willi",
                        "Stephan <- Willi -> null"
                )
        );
    }

    // TODO with-clause
    // TODO common-module with User & Role

    private static class User {
        public Integer id;
        public String name;
        public String email;

        public User(Integer id, String name, String email) {
            this.id = id;
            this.name = name;
            this.email = email;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            User user = (User) o;
            return Objects.equals(id, user.id) &&
                    Objects.equals(name, user.name) &&
                    Objects.equals(email, user.email);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name, email);
        }

        @Override
        public String toString() {
            return "User{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", email='" + email + '\'' +
                    '}';
        }
    }

    private static class Role {
        public Integer id;
        public String name;

        public Role(Integer id, String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Role role = (Role) o;
            return Objects.equals(id, role.id) &&
                    Objects.equals(name, role.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name);
        }

        @Override
        public String toString() {
            return "Role{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    '}';
        }
    }
}