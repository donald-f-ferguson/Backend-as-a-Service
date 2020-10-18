import json
import Samples.dynamo.dynamodb as db
import uuid

def load_json(fn):

    result = []
    with open(fn, "r") as infile:
        result = json.load(infile)

    return result


def save_comments(comms):

    for c in comms:
        db.put_item("comments", c)




def load_all():
    comms = load_json("./comments.json")
    rsps = load_json("./responses.json")

    next_rsp = 0;

    for c in comms:
        c['tags'] = [c.get('tags', [])]
        c['version_id'] = str(uuid.uuid4())
        rs_cnt = len(c['responses'])
        c['responses'] = []

        for i in range(0,rs_cnt):
            nr = rsps[next_rsp]
            nr['version_id'] = str(uuid.uuid4())
            c['responses'].append(nr)
            next_rsp += 1

    save_comments(comms)


def scan_all():
    rsp = db.do_a_scan("comments", None)
    print("Scan response = \n", json.dumps(rsp, indent=3, default=str))


def test_add_response():
    email = "foo@exam.org"
    table="comments",
    rsp = "Totally cool! Add ID fields this time!"
    comment_id = "01cdb10e-6d9b-4b23-98bc-db062ae908ec"

    res = db.add_response("comments", commenter_email=email, comment_id=comment_id, response=rsp)


def test_filter():

    filter = {"email": "dff9"}
    res = db.find_by_template("comments", filter)
    print("test_filter: result = \n", json.dumps(res, indent=3))


def test_add_comment():

    res = db.add_comment("dff9", "Everything is awesome?", ["cool", "definitely"])
    print("test_add_comment result = \n", res)

#scan_all()
#load_all()
#test_add_response()

#scan_all()
test_filter()

#test_add_comment()