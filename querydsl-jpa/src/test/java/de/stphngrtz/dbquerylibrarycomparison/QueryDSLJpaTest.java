package de.stphngrtz.dbquerylibrarycomparison;

import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 * http://www.querydsl.com/static/querydsl/latest/reference/html/ch02.html
 *
 * Disclaimer: Queries might look strange. This is because my goal was to test the functionality, not to write good queries ;)
 */
public class QueryDSLJpaTest {

    private static EntityManager entityManager;
    private static JPAQueryFactory queryFactory;

    @BeforeClass
    public static void setUpClass() throws Exception {
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("QueryDSLJpa");
        entityManager = emf.createEntityManager();
        queryFactory = new JPAQueryFactory(entityManager);
    }

    @Before
    public void setUp() throws Exception {
        entityManager.getTransaction().begin();
    }

    @After
    public void tearDown() throws Exception {
        entityManager.getTransaction().rollback();
    }

    @Test
    public void selectAll() throws Exception {
        QUserJPA qUserJPA = QUserJPA.userJPA;

        assertThat(queryFactory
                .selectFrom(qUserJPA)
                .fetch(),
                containsInAnyOrder(
                        new UserJPA(1, "Stephan", "stephan.goertz@gmail.com"),
                        new UserJPA(2, "Steffi", "steffi05.04@freenet.de"),
                        new UserJPA(3, "Willi", "willi@web.de"),
                        new UserJPA(4, "Franz", "franz@web.de")
                )
        );
    }

    @Test
    public void selectAllOrderedBy() throws Exception {
        QRoleJPA qRoleJPA = QRoleJPA.roleJPA;
        assertThat(queryFactory
                        .selectFrom(qRoleJPA)
                        .orderBy(qRoleJPA.name.desc())
                        .fetch(),
                contains(
                        new RoleJPA(2, "Developer"),
                        new RoleJPA(3, "Designer"),
                        new RoleJPA(1, "Admin")
                )
        );
    }

    @Test
    public void selectWhereEquals() throws Exception {
        QUserJPA qUserJPA = QUserJPA.userJPA;
        assertThat(queryFactory
                        .selectFrom(qUserJPA)
                        .where(qUserJPA.id.eq(1))
                        .fetch(),
                contains(
                        new UserJPA(1, "Stephan", "stephan.goertz@gmail.com")
                )
        );
    }

    @Test
    public void selectWhereLike() throws Exception {
        QUserJPA qUserJPA = QUserJPA.userJPA;
        assertThat(queryFactory
                        .selectFrom(qUserJPA)
                        .where(qUserJPA.email.like("%@gmail.com"))
                        .fetch(),
                contains(
                        new UserJPA(1, "Stephan", "stephan.goertz@gmail.com")
                )
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void selectWithConstant() throws Exception {
        QUserJPA qUserJPA = QUserJPA.userJPA;
        queryFactory
                .select(qUserJPA.id, qUserJPA.name, Expressions.constant("mail@me.de"))
                .from(qUserJPA)
                .where(qUserJPA.id.eq(1))
                .fetch()
                .stream().map(t -> new UserJPA(t.get(qUserJPA.id), t.get(qUserJPA.name), t.get(2, String.class))).collect(Collectors.toList());
    }

    @Test
    public void selectWithSubselectInSelectBlock() throws Exception {
        QUserJPA qUserJPA = QUserJPA.userJPA;
        QRoleJPA qRoleJPA = QRoleJPA.roleJPA;
        assertThat(queryFactory
                        .select(qUserJPA.name, JPAExpressions.select(qRoleJPA.count()).from(qRoleJPA).where(qRoleJPA.user.contains(qUserJPA)))
                        .from(qUserJPA)
                        .fetch()
                        .stream().map(t -> t.get(qUserJPA.name) + " (" + t.get(1, Integer.class) + ")").collect(Collectors.toList()),
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
        QUserJPA qUserJPA1 = new QUserJPA("users1");
        QUserJPA qUserJPA2 = new QUserJPA("users2");
        assertThat(queryFactory
                        .select(qUserJPA1.name)
                        .from(qUserJPA1)
                        .where(qUserJPA1.in(JPAExpressions.select(qUserJPA2).from(qUserJPA2).where(qUserJPA2.roles.isNotEmpty())))
                        .fetch(),
                containsInAnyOrder(
                        "Stephan",
                        "Steffi",
                        "Franz"
                )
        );
    }

    @Test(expected = UnsupportedOperationException.class)
    @SuppressWarnings("unchecked")
    public void selectUnionSelect() throws Exception {
        throw new UnsupportedOperationException("QueryDSL-JPA doesn't support UNION (afaik)");
    }

    @Test
    public void selectWithImplicitJoin() throws Exception {
        QUserJPA qUserJPA = QUserJPA.userJPA;
        QRoleJPA qRoleJPA = QRoleJPA.roleJPA;
        assertThat(queryFactory
                        .select(qUserJPA.name, qRoleJPA.name)
                        .from(qUserJPA, qRoleJPA)
                        .where(qUserJPA.roles.contains(qRoleJPA))
                        .fetch()
                        .stream().map(t -> t.get(qUserJPA.name) + " ist " + t.get(qRoleJPA.name)).collect(Collectors.toList()),
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
        QUserJPA qUserJPA = QUserJPA.userJPA;
        QRoleJPA qRoleJPA = QRoleJPA.roleJPA;
        assertThat(queryFactory
                        .select(qUserJPA.name, qRoleJPA.name)
                        .from(qUserJPA)
                        .join(qUserJPA.roles, qRoleJPA)
                        .fetch()
                        .stream().map(t -> t.get(qUserJPA.name) + " ist " + t.get(qRoleJPA.name)).collect(Collectors.toList()),
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
        QUserJPA qUserJPA = QUserJPA.userJPA;
        QRoleJPA qRoleJPA = QRoleJPA.roleJPA;
        assertThat(queryFactory
                        .select(qUserJPA.name, qRoleJPA.name)
                        .from(qUserJPA)
                        .leftJoin(qUserJPA.roles, qRoleJPA)
                        .fetch()
                        .stream().map(t -> t.get(qUserJPA.name) + " ist " + t.get(qRoleJPA.name)).collect(Collectors.toList()),
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
        QRoleJPA qRoleJPA = QRoleJPA.roleJPA;
        QUserJPA qUserJPA = QUserJPA.userJPA;
        assertThat(queryFactory
                        .select(qRoleJPA.name, qUserJPA.count())
                        .from(qRoleJPA)
                        .join(qRoleJPA.user, qUserJPA)
                        .groupBy(qRoleJPA.name)
                        .fetch()
                        .stream().map(t -> t.get(qRoleJPA.name) + " mit " + t.get(qUserJPA.count()) + " User(n)").collect(Collectors.toList()),
                containsInAnyOrder(
                        "Admin mit 1 User(n)",
                        "Developer mit 2 User(n)",
                        "Designer mit 1 User(n)"
                )
        );
    }

    @Test
    public void selectCountWithGroupByHaving() throws Exception {
        QRoleJPA qRoleJPA = QRoleJPA.roleJPA;
        QUserJPA qUserJPA = QUserJPA.userJPA;
        assertThat(queryFactory
                        .select(qRoleJPA.name, qUserJPA.count())
                        .from(qRoleJPA)
                        .join(qRoleJPA.user, qUserJPA)
                        .groupBy(qRoleJPA.name)
                        .having(qUserJPA.count().gt(1))
                        .fetch()
                        .stream().map(t -> t.get(qRoleJPA.name) + " mit " + t.get(qUserJPA.count()) + " User(n)").collect(Collectors.toList()),
                contains(
                        "Developer mit 2 User(n)"
                )
        );
    }

    @Test(expected = UnsupportedOperationException.class)
    public void window() throws Exception {
        throw new UnsupportedOperationException("QueryDSL-JPA doesn't support window-functions (afaik)");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void multipleWindows() throws Exception {
        throw new UnsupportedOperationException("QueryDSL-JPA doesn't support window-functions (afaik)");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void selectFromSimpleCommonTable() throws Exception {
        throw new UnsupportedOperationException("QueryDSL-JPA doesn't support WITH (afaik)");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void selectFromMoreComplexCommonTable() throws Exception {
        throw new UnsupportedOperationException("QueryDSL-JPA doesn't support WITH (afaik)");
    }

    @Test
    public void dmlStatements() throws Exception {
        QUserJPA qUserJPA = QUserJPA.userJPA;

        entityManager.persist(new UserJPA(5, "Test", "test@mail.de"));

        assertThat(queryFactory
                        .update(qUserJPA)
                        .set(qUserJPA.email, "test@web.de")
                        .where(qUserJPA.id.eq(5))
                        .execute(),
                equalTo(1L)
        );
        assertThat(queryFactory
                        .update(qUserJPA)
                        .set(qUserJPA.name, "Testuser")
                        .where(qUserJPA.email.eq("test@mail.de"))
                        .execute(),
                equalTo(0L)
        );
        assertThat(queryFactory
                        .delete(qUserJPA)
                        .where(qUserJPA.id.eq(5))
                        .execute(),
                equalTo(1L)
        );
    }

    @Test(expected = UnsupportedOperationException.class)
    public void dmlBatchStatements() throws Exception {
        throw new UnsupportedOperationException("QueryDSL-JPA doesn't support BATCH (afaik)");
    }

    // TODO Using Native SQL in JPA queries (http://www.querydsl.com/static/querydsl/latest/reference/html/ch02.html#d0e426)
}