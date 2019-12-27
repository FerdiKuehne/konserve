(ns konserve.core-test
  (:refer-clojure :exclude [get get-in update update-in assoc assoc-in dissoc exists? keys])
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [<!! go] :as async]
            [konserve.core :refer :all]
            [konserve.memory :refer [new-mem-store]]
            [konserve.filestore :refer [new-fs-store delete-store list-keys]]
            [clojure.java.io :as io]))

(deftest memory-store-test
  (testing "Memory Store"
    (compliance-test (fn [folder] (<!! (new-mem-store))) (fn [folder]) (fn [folder]))))

(deftest append-store-test
  (testing "Test the append store functionality."
    (let [store (<!! (new-mem-store))]
      (<!! (append store :foo {:bar 42}))
      (<!! (append store :foo {:bar 43}))
      (is (= (<!! (log store :foo))
             '({:bar 42}{:bar 43})))
      (is (= (<!! (reduce-log store
                              :foo
                              (fn [acc elem]
                                (conj acc elem))
                              []))
             [{:bar 42} {:bar 43}])))))

(deftest filestore-test
  (testing "Filestore"
    (compliance-test (fn [folder] (<!! (new-fs-store folder))) (fn [folder] (delete-store folder)) (fn [store] (list-keys store)))))

