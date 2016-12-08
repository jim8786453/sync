import sync


class Sync(object):

    def process_response(self, req, resp, resource, req_succeeded):
        sync.close()
