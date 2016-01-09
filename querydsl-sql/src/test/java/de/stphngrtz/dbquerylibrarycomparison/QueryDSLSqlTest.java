package de.stphngrtz.dbquerylibrarycomparison;

import com.querydsl.core.Tuple;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.PathBuilder;
import com.querydsl.sql.*;
import org.junit.BeforeClass;
import org.junit.Test;
import org.postgresql.ds.PGPoolingDataSource;

import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * http://www.querydsl.com/static/querydsl/latest/reference/html/ch02s03.html
 *
 * Disclaimer: Queries might look strange. This is because my goal was to test the functionality, not to write good queries ;)
 */
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
        configuration.addListener(new SQLBaseListener() {
            @Override
            public void preExecute(SQLListenerContext context) {
                System.out.println(context.getSQL());
            }
        });

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
        QUsers qUsers = QUsers.users;
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
        QRoles qRoles = QRoles.roles;
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
        QUsers qUsers = QUsers.users;
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
        QUsers qUsers = QUsers.users;
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
        QUsers qUsers = QUsers.users;
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
        QUsers qUsers = QUsers.users;
        QUsersWithRoles qUsersWithRoles = QUsersWithRoles.usersWithRoles;
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
        QUsers qUsers = QUsers.users;
        QUsersWithRoles qUsersWithRoles = QUsersWithRoles.usersWithRoles;
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
        QUsers qUsers = QUsers.users;
        QRoles qRoles = QRoles.roles;
        QUsersWithRoles qUsersWithRoles = QUsersWithRoles.usersWithRoles;
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
        QUsers qUsers = QUsers.users;
        QRoles qRoles = QRoles.roles;
        QUsersWithRoles qUsersWithRoles = QUsersWithRoles.usersWithRoles;
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
        QUsers qUsers = QUsers.users;
        QRoles qRoles = QRoles.roles;
        QUsersWithRoles qUsersWithRoles = QUsersWithRoles.usersWithRoles;
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
        QRoles qRoles = QRoles.roles;
        QUsersWithRoles qUsersWithRoles = QUsersWithRoles.usersWithRoles;
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
        QRoles qRoles = QRoles.roles;
        QUsersWithRoles qUsersWithRoles = QUsersWithRoles.usersWithRoles;
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
        QUsers qUsers = QUsers.users;
        assertThat(queryFactory
                        .select(
                                qUsers.name,
                                SQLExpressions.left(qUsers.name, 1),
                                SQLExpressions.count().over().partitionBy(SQLExpressions.left(qUsers.name, 1))
                        )
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
        QUsers qUsers = QUsers.users;
        assertThat(queryFactory
                        .select(
                                SQLExpressions.lag(qUsers.name).over().orderBy(qUsers.name.asc()),
                                qUsers.name,
                                SQLExpressions.lead(qUsers.name).over().orderBy(qUsers.name.asc())
                        )
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

    @Test
    public void selectFromSimpleCommonTable() throws Exception {
        QUsers qUsers = QUsers.users;
        assertThat(queryFactory.query()
                        .with(qUsers, SQLExpressions
                                .select(qUsers.name, qUsers.email)
                                .from(qUsers)
                                .where(qUsers.name.startsWith("S"))
                        )
                        .select(qUsers.name, qUsers.email)
                        .from(qUsers)
                        .fetch()
                        .stream().map(t -> t.get(qUsers.name) + " (" + t.get(qUsers.email) + ")").collect(Collectors.toList()),
                containsInAnyOrder(
                        "Stephan (stephan.goertz@gmail.com)",
                        "Steffi (steffi05.04@freenet.de)"
                )
        );
    }

    @Test
    public void selectFromMoreComplexCommonTable() throws Exception {
        QUsers qUsers = QUsers.users;
        QRoles qRoles = QRoles.roles;
        QUsersWithRoles qUsersWithRoles = QUsersWithRoles.usersWithRoles;
        PathBuilder<Tuple> common = new PathBuilder<>(Tuple.class, "common");

        assertThat(queryFactory.query()
                        .with(common, SQLExpressions
                                .select(qUsers.name.as("userName"), qRoles.name.as("roleName"))
                                .from(qUsers)
                                .leftJoin(qUsersWithRoles).on(qUsersWithRoles.userId.eq(qUsers.id))
                                .leftJoin(qRoles).on(qRoles.id.eq(qUsersWithRoles.roleId))
                        )
                        .select(common.get("userName"), common.get("roleName"))
                        .from(common)
                        .where(common.getString("userName").startsWith("S"))
                        .fetch().stream().map(t -> t.get(common.getString("userName")) + " ist " + t.get(common.getString("roleName"))).collect(Collectors.toList()),
                containsInAnyOrder(
                        "Stephan ist Admin",
                        "Stephan ist Developer",
                        "Steffi ist Designer"
                )
        );
    }

    @Test
    public void dmlStatements() throws Exception {
        QUsers qUsers = QUsers.users;

        assertThat(queryFactory
                        .insert(qUsers)
                        .columns(qUsers.id, qUsers.name, qUsers.email)
                        .values(5, "Test", "test@mail.de")
                        .execute(),
                equalTo(1L)
        );
        assertThat(queryFactory
                        .update(qUsers)
                        .set(qUsers.email, "test@web.de")
                        .where(qUsers.id.eq(5))
                        .execute(),
                equalTo(1L)
        );
        assertThat(queryFactory
                        .update(qUsers)
                        .set(qUsers.name, "Testuser")
                        .where(qUsers.email.eq("test@mail.de"))
                        .execute(),
                equalTo(0L)
        );
        assertThat(queryFactory
                        .delete(qUsers)
                        .where(qUsers.id.eq(5))
                        .execute(),
                equalTo(1L)
        );
    }

    @Test
    public void dmlBatchStatements() throws Exception {
        QUsers qUsers = QUsers.users;

        assertThat(queryFactory
                        .insert(qUsers)
                        .columns(qUsers.id, qUsers.name, qUsers.email)
                        .values(5, "Test 1", "test1@mail.de").addBatch()
                        .values(6, "Test 2", "test2@mail.de").addBatch()
                        .execute(),
                equalTo(2L)
        );
        assertThat(queryFactory
                        .update(qUsers)
                        .set(qUsers.name, "Testuser 1").where(qUsers.id.eq(5)).addBatch()
                        .set(qUsers.name, "Testuser 2").where(qUsers.id.eq(6)).addBatch()
                        .execute(),
                equalTo(2L)
        );
        assertThat(queryFactory
                        .delete(qUsers)
                        .where(qUsers.id.eq(5)).addBatch()
                        .where(qUsers.id.eq(6)).addBatch()
                        .execute(),
                equalTo(2L)
        );
    }
}