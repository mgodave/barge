(ns barge.jepsen.test
  (:use barge.jepsen.system
        jepsen.core
        jepsen.tests
        clojure.test
        clojure.pprint)
  (:require [clojure.string   :as str]
            [jepsen.util      :as util]
            [jepsen.os.debian :as debian]
            [jepsen.checker   :as checker]
            [jepsen.checker.timeline :as timeline]
            [jepsen.model     :as model]
            [jepsen.generator :as gen]
            [jepsen.nemesis   :as nemesis]
            [jepsen.store     :as store]
            [jepsen.report    :as report]))

(def the-db
  (atom nil))

(deftest register-test
 (let [test (run!
              (assoc
                noop-test
                :name      "barge"
                :os        debian/os
                :db        (atom-db the-db)
                :client    (atom-client the-db)
                :model     (model/set)
                :checker   (checker/compose {:html timeline/html
                                             :set  checker/set})
                :nemesis   (nemesis/partitioner nemesis/bridge)
                :generator (gen/phases
                             (->> (range)
                                  (map (fn [x] {:type  :invoke
                                                :f     :add
                                                :value x}))
                                  gen/seq
                                  (gen/stagger 1/10)
                                  (gen/delay 1)
                                  (gen/nemesis
                                    (gen/seq
                                      (cycle [(gen/sleep 60)
                                              {:type :info :f :start}
                                              (gen/sleep 300)
                                              {:type :info :f :stop}])))
                                  (gen/time-limit 600))
                             (gen/nemesis
                               (gen/once {:type :info :f :stop}))
                             (gen/clients
                               (gen/once {:type :invoke :f :read})))))]
   (is (:valid? (:results test)))
   (report/linearizability (:linear (:results test)))
   (pprint (:results test))))


(gen/ops  test [1 2 3 4 5] range)

