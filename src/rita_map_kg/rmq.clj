(ns rita-map-kg.rmq
  (:require [libpython-clj2.python :as py]
            [libpython-clj2.require :refer [require-python]]))

;; Initialize Python environment and add your Python file path
(defn init-python []
  (py/initialize!)
  (py/run-simple-string "import sys")
  (py/run-simple-string "sys.path.append('./resources')")
  (require-python 'rmq))

;; Initialize RMQ instance
(def rmq-instance (atom nil))

(defn init-rmq-instance [config]
  (let [Rmq (py/import-module "rmq")]
    (reset! rmq-instance (py/call-attr Rmq "Rmq" (:exchange config) (:host config) (:port config)))
    (println "RMQ instance initialized.")))

;; Send RMQ messages using the reusable RMQ instance
(defn send-rmq-message [config message]
  (if-let [instance @rmq-instance]
    (do
      (py/call-attr instance "publish_message" (:routing-key config) (py/->python message))
      (println "Message sent"))
    (println "Error: RMQ instance not initialized.")))


;; Close RMQ connection
(defn close-rmq-connection []
  (if-let [instance @rmq-instance]
    (do
      (py/call-attr instance "close_connection")
      (println "RMQ connection closed."))
    (println "Error: RMQ instance not initialized.")))

