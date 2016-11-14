#[cfg(feature="web")]
extern crate distributary;
#[cfg(feature="web")]
extern crate shortcut;

use std::collections::HashMap;

#[cfg(feature="web")]
fn main() {
    use distributary::*;

    // set up graph
    let mut g = distributary::FlowGraph::new();

    // HotCRP has users and papers
    let user = g.incorporate(new("user",
                                 &["uid", "name", "email", "affiliation", "is_chair"],
                                 true,
                                 Base {}));
    let paper = g.incorporate(new("paper",
                                  &["pid", "title", "status", "content"],
                                  true,
                                  Base {}));
    // but we also have associations between these, such as Author and Review
    let author = g.incorporate(new("author", &["uid", "pid"], true, Base {}));
    let review = g.incorporate(new("review", &["rid", "uid", "pid", "content"], true, Base {}));

    // all papers with authors
    let j = JoinBuilder::new(vec![(author, 0), (paper, 0), (paper, 1), (paper, 2)])
        .from(author, vec![0, 1])
        .join(paper, vec![1, 0, 0, 0]);
    let authored = g.incorporate(new("authored", &["uid", "pid", "title", "status"], true, j));

    // chairs should have at least as much access as authors to all papers
    let j = JoinBuilder::new(vec![(user, 0), (paper, 0)])
        .from(user, vec![0, 0, 0, 0, 0])
        .join(paper, vec![0, 0, 0, 0]);
    let chairs = g.incorporate(new("chairs", &["uid", "pid"], true, j));

    // let's define some security policies too
    // chairs and authors can see author lists
    let mut emits = HashMap::new();
    emits.insert(author, vec![0, 1]);
    emits.insert(chairs, vec![0, 1]);
    let can_see_authors =
        g.incorporate(new("see_authors", &["uid", "pid"], false, Union::new(emits)));

    // a given review can be seen by a) the chairs; b) the reviewer; and c) the authors *if* the
    // reviews have been released. let's construct a view for c.
    let j = JoinBuilder::new(vec![(review, 0), (authored, 1), (authored, 0), (authored, 3)])
        .from(authored, vec![0, 1])
        .join(review, vec![0, 0, 1, 0]);
    let visible_reviews =
        g.incorporate(new("visible_reviews", &["rid", "pid", "uid", "status"], true, j)
            .having(vec![shortcut::Condition {
                             column: 3,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const("accepted"
                                 .into())), // should be != pending
                         }]));
    // and then the policy view
    let mut emits = HashMap::new();
    // emits.insert(chairs, vec![0, 1]); we need chairs * review -- ugh
    emits.insert(review, vec![0, 1]);
    emits.insert(visible_reviews, vec![0, 2]);
    let can_see_review =
        g.incorporate(new("see_review", &["uid", "rid"], false, Union::new(emits)));

    // chairs, reviewers, and authors can see papers
    let mut emits = HashMap::new();
    emits.insert(author, vec![0, 1]);
    emits.insert(chairs, vec![0, 1]);
    emits.insert(review, vec![1, 2]);
    let can_see_paper = g.incorporate(new("see_paper", &["uid", "pid"], false, Union::new(emits)));

    // visible reviews for a given user
    let j = JoinBuilder::new(vec![(can_see_review, 0),
                                  (review, 0),
                                  (review, 1),
                                  (review, 2),
                                  (review, 3)])
        .from(can_see_review, vec![0, 1])
        .join(review, vec![1, 0, 0, 0]);
    let visible_reviews = g.incorporate(new("visible_reviews",
                                            &["viewer", "rid", "uid", "pid", "content"],
                                            true,
                                            j));

    // "my" reviews should also show paper title
    // TODO: we'd really like to restrict this to viewer == uid, but not good way to do so
    let j = JoinBuilder::new(vec![(visible_reviews, 0),
                                  (visible_reviews, 1),
                                  (visible_reviews, 2),
                                  (paper, 0),
                                  (visible_reviews, 4),
                                  (paper, 1)])
        .from(visible_reviews, vec![0, 0, 0, 1])
        .join(paper, vec![1, 0, 0, 0]);
    let my_reviews = g.incorporate(new("my_reviews",
                                       &["viewer", "rid", "uid", "pid", "content", "title"],
                                       true,
                                       j));


    // run it!
    web::run(g).unwrap();
}

#[cfg(not(feature="web"))]
fn main() {
    unreachable!("compile with --features=web to build the web frontend");
}
