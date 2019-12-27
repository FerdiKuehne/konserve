(ns konserve.filestore-test
  (:require [konserve.core :as k]
            [cljs.test :refer-macros [deftest is testing async use-fixtures run-tests]]
            [cljs.core.async :as async :refer (take! <! >! put! take! close! chan poll!)]
            [konserve.filestore :refer [new-fs-store delete-store list-keys]]
            [cljs.nodejs :as node])
  (:require-macros [cljs.core.async.macros :refer [go]]))

(defonce fs (node/require "fs"))

(defonce stream (node/require "stream"))

(enable-console-print!)

(use-fixtures :once
  {:before
   (fn []
     (async done
            (go (delete-store "/tmp/konserve-fs-nodejs-test")
                (def store (<! (new-fs-store "/tmp/konserve-fs-nodejs-test")))
                (done))))})

(deftest filestore-test
  (testing "Test the file store functionality."
    (async done
           (go
             (k/compliance-test (fn [folder] store) (fn [folder] ) (fn [folder] ))
             (done)))))

