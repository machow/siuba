# TODO: dot, corr, cov
# ordered set aggregate. e.g. mode()
# hypothetical-set aggregate (e.g. rank(a) as if it were in partition(order_by b))

# kinds of windows:
#   * result len n_elements: rank()
#   * result len 1: is_monotonic (lag, diff, and any). ordered set aggregate.
#   * result len input len: percentile_cont([.1, .2]). hypo set aggregate.

from .utils import enrich_spec_entry
import yaml
import pkg_resources

fname_spec = pkg_resources.resource_filename("siuba.spec", "series.yml")
with open(fname_spec, "r") as f:
    raw_spec = yaml.load(f, Loader = yaml.SafeLoader)
    spec = {k: enrich_spec_entry(entry) for k, entry in raw_spec.items()}
