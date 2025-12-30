#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <tuple>

#include "alloc.h"
#include "log.h"
#include "resrc.h"

// #include "tenant.h"

namespace py = pybind11;

using namespace hare;

PYBIND11_MODULE(hare_alloc_engine, m) {
  m.doc() = "HopperKV allocator";

  py::class_<StatelessResrcVec>(m, "StatelessResrcVec")
      .def(py::init<double, double, double>())
      .def(py::init<const std::tuple<double, double, double> &>())
      .def("to_string", &StatelessResrcVec::to_string)
      .def("to_tuple", &StatelessResrcVec::to_tuple);

  py::class_<ResrcVec>(m, "ResrcVec")
      .def(py::init<uint64_t, double, double, double>())
      .def(py::init<uint64_t, StatelessResrcVec>())
      .def(py::init<const std::tuple<uint64_t, double, double, double> &>())
      .def("to_string", &ResrcVec::to_string)
      .def("to_tuple", &ResrcVec::to_tuple);

  py::class_<MissRatioCurve>(m, "MissRatioCurve")
      .def(py::init<const std::vector<uint64_t> &,
                    const std::vector<double> &>())
      .def("get_miss_ratio", &MissRatioCurve::get_miss_ratio);

  py::class_<Allocator>(m, "Allocator")
      .def(py::init<bool, bool, bool>(), py::arg("harvest") = true,
           py::arg("conserving") = true, py::arg("memshare") = false)
      .def("add_tenant", &Allocator::add_tenant)
      .def("do_alloc", &Allocator::do_alloc)
      .def("get_alloc_result", &Allocator::get_alloc_result);

  // allocator params set/get
  m.def("get_policy_alloc_total_net_bw",
        &params::policy::get_alloc_total_net_bw);
  m.def("set_policy_alloc_total_net_bw",
        &params::policy::set_alloc_total_net_bw);

  m.def("set_cache_delta", &params::alloc::set_cache_delta);
  m.def("set_min_cache_size", &params::alloc::set_min_cache_size);
  m.def("set_min_db_rcu", &params::alloc::set_min_db_rcu);
  m.def("set_min_db_wcu", &params::alloc::set_min_db_wcu);
  m.def("set_min_net_bw", &params::alloc::set_min_net_bw);

  m.def("get_cache_delta", &params::alloc::get_cache_delta);
  m.def("get_min_cache_size", &params::alloc::get_min_cache_size);
  m.def("get_min_db_rcu", &params::alloc::get_min_db_rcu);
  m.def("get_min_db_wcu", &params::alloc::get_min_db_wcu);
  m.def("get_min_net_bw", &params::alloc::get_min_net_bw);

  m.def("config_logger", &log::config_logger);
}
