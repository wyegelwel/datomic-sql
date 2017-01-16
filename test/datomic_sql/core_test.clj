(ns datomic-sql.core-test
  (:require [clojure.test :refer :all]
            [datomic-sql.core :refer :all]
            [datomic.api :as d]
            [oj.core :as oj]
            [oj.modifiers :as db]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [plumbing.core :refer (map-keys map-vals)]))


(deftest datomic-attribute->table-field-test
  (testing "Example test"
    (is (= (datomic-attribute->table-field :bookmarks/title)
           {:table :bookmarks
            :field :title}))
    (is (= (datomic-attribute->table-field :bookmarks.test/title)
           {:table :bookmarks.test
            :field :title}))))

(deftest q->parts-test
  (testing "Example test"
    (is (= (q->parts '[:find ?title
                       :in $ ?type
                       :where 
                       [?e :bookmarks/type ?type]
                       [?e :bookmarks/title ?title]])
           '{:find (?title)
             :in ($ ?type)
             :where ([?e :bookmarks/type ?type] [?e :bookmarks/title ?title])}))))

(deftest dissect-where-test
  (testing "Example test"
    (is (= (dissect-where '[?e :bookmarks/type ?type])
           '{:e ?e
             :a :bookmarks/type
             :v ?type
             :t nil}))))

(deftest where-maps->var-map-test
  (testing "Example test"
    (is (= (where-maps->var-map '[{:e ?e :a :bookmarks/type :v ?type :t nil}
                                  {:e ?e :a :bookmarks/title :v ?title :t nil}])
           '{?type ({:type :v
                     :e ?e
                     :a :bookmarks/type
                     :v ?type
                     :t nil})
             ?title ({:type :v
                      :e ?e
                      :a :bookmarks/title
                      :v ?title
                      :t nil})
             ?e ({:type :e
                  :e ?e
                  :a :bookmarks/type
                  :v ?type
                  :t nil}
                 {:type :e
                  :e ?e
                  :a :bookmarks/title
                  :v ?title
                  :t nil})}))))



(deftest q-parts->resolved-vars-test
  (testing "Example test"
    (is (= (q-parts->resolved-vars '{:find (?title)
                                     :in ($ ?type)
                                     :where ([?e :bookmarks/type ?type] 
                                             [?e :bookmarks/title ?title])})
           '{?title {:find true
                     :in false
                     :field :title
                     :where [{:type :v
                              :e ?e
                              :a :bookmarks/title
                              :v ?title
                              :t nil
                              }]}
             ?type {:find false
                    :in true
                    :in-index 1
                    :field :type
                    :where [{:type :v
                             :e ?e
                             :a :bookmarks/type
                             :v ?type
                             :t nil
                             }]} 
             $ {:find false :field nil :in true :where nil :in-index 0}
             ?e {:find false
                 :in false
                 :field :id
                 :where [
                         {:type :e
                          :e ?e
                          :a :bookmarks/type
                          :v ?type
                          :t nil
                          }
                         {:type :e
                          :e ?e
                          :a :bookmarks/title
                          :v ?title
                          :t nil
                          }
                         ]}}))))

(deftest should-ignore?-test
  (testing "Example tests"
    (is (= (should-ignore? '$)
           true))
    (is (= (should-ignore? '_)
           true))
    (is (= (should-ignore? '?test)
           false))))

(deftest build-sql-where-test
  (testing "Example test"
    (is (= (build-sql-where '{:find false
                              :in true
                              :in-index 1
                              :field :type
                              :where [{:type :v
                                       :e ?e
                                       :a :bookmarks/type
                                       :v ?type}]}
                            '[db-conn "web"])
           {:type "web"}))))

(deftest build-sql-test
  (testing "Example test"
    (is (= (build-sql '{:find (?title)
                        :in ($ ?type)
                        :where 
                        ([_ :bookmarks/type ?type]
                         [_ :bookmarks/title ?title])}
                      '{?title {:find true
                                :in false
                                :field :title
                                :where [{:type :v
                                         :e _
                                         :a :bookmarks/title
                                         :v ?title}]}
                        ?type {:find false
                               :in true
                               :field :type
                               :in-index 1
                               :where [{:type :v
                                        :e _
                                        :a :bookmarks/type
                                        :v ?type}]} 
                        $ {:find false :in true :where nil}
                        _ {:find false
                           :in false
                           :where [{:type :e
                                    :e _
                                    :a :bookmarks/title
                                    :v ?title}
                                   {:type :v
                                    :e _
                                    :a :bookmarks/type
                                    :v ?type}]}}
                      '[db "web"])
           {:table :bookmarks
            :select [:title]
            :where {:type "web"}}))))

(defn setup-test-sql-db 
  []
  (io/delete-file "test.db" true)
  (let [db  {:classname "org.sqlite.JDBC" :subprotocol "sqlite" :subname "test.db"} 
        make-bookmark-table (fn make-bookmark-table [test-db]
                              (jdbc/with-db-transaction [db test-db]
                                (jdbc/db-do-prepared 
                                  db
                                  (jdbc/create-table-ddl :bookmarks
                                                         [:id :integer :primary :key :autoincrement]
                                                         [:type "longvarchar"]
                                                         [:title "longvarchar"]))))]
    (make-bookmark-table db)
    db))

(defn transact-sql-data
  [table data db]
  (doseq [d data]
    (oj/exec {:table table
              :insert d}
             db)))

(defn create-datomic-db-conn 
    ([]
     (create-datomic-db-conn "datomic:mem://test"))
    ([db-uri]
     (d/delete-database db-uri)
     (d/create-database db-uri)
     (d/connect db-uri)))

(defn setup-datomic-conn
  []
  (let [dconn (create-datomic-db-conn)
        make-datomic-bookmark-schema (fn make-datomic-bookmark-schema 
                                       [dconn]
                                       (d/transact dconn
                                                   [{:db/id #db/id [:db.part/db]
                                                     :db/ident :bookmarks/type
                                                     :db/valueType :db.type/string
                                                     :db/cardinality :db.cardinality/one
                                                     :db.install/_attribute :db.part/db}
                                                    {:db/id #db/id [:db.part/db]
                                                     :db/ident :bookmarks/title
                                                     :db/valueType :db.type/string
                                                     :db/cardinality :db.cardinality/one
                                                     :db.install/_attribute :db.part/db}]))]

    (make-datomic-bookmark-schema dconn)
    dconn))

(defn transact-datomic-data
  [table data conn]
  (doseq [d data]
    (d/transact conn [(map-keys #(keyword (name table) (name %)) d)])))

(deftest q-test
  (testing "same as datomic"
    (let [data [{:type "web"
                 :title "my-cool-title"}
                {:type "web"
                 :title "my-cool-title2"}]
          sql-db (setup-test-sql-db)
          datomic-conn (setup-datomic-conn)]
      (transact-sql-data :bookmarks data sql-db)
      (transact-datomic-data :bookmarks data datomic-conn)
      (is (= (d/q '[:find ?title
                    :in $ ?type
                    :where 
                    [_ :bookmarks/type ?type]
                    [_ :bookmarks/title ?title]]
                  (d/db datomic-conn) "web")
             (q '[:find ?title
                  :in $ ?type
                  :where 
                  [_ :bookmarks/type ?type]
                  [_ :bookmarks/title ?title]]
                sql-db "web"))))))
