(ns datomic-sql.core
  (:require [oj.core :as oj]
            [oj.modifiers :as db]
            [clojure.java.jdbc :as jdbc]
            [plumbing.core :refer (map-keys map-vals)]))

;;TODO: Provide a way to override convention
(defn datomic-attribute->table-field 
  "This assumes the convention on datomic attributes
   :<table>/<field>
   
   Takes the attribute and returns a map with keys [:table and :field]

   Example:
   (datomic-attribute->table-field :bookmarks/title)
   {:table :bookmarks
    :field :title}"
  [datomic-attribute]
  {:table (keyword (namespace datomic-attribute))
   :field (keyword (name datomic-attribute))}) 

(defn q->parts 
  "Takes a datomic query using the spec from datomic.api/q
   and returns a map containing keys [:find :where :in]
   and the values are lists of values for each part.

   Example:
   (q->parts '[:find ?title
               :in $ ?type
               :where 
               [?e :bookmarks/type ?type]
               [?e :bookmarks/title ?title]])
   {:find (?title)
    :in ($ ?type)
    :where ([?e :bookmarks/type ?type] [?e :bookmarks/title ?title])}"
  [query]
  (->> query
       (partition-by #{:find :where :in})
       (partition 2)
       (map vec)
       (into {})
       (map-keys first)))

(defn dissect-where
  "Takes a datomic where clause of the form [e a v] or [e a v t]
   and returns a map with keys [:e :a :v :t] where the values are 
   the [e a v t] in the clause
   
   Example:
   (dissect-where '[?e :bookmarks/type ?type])
   {:e ?e
    :a :bookmarks/type
    :v ?type
    :t nil}"
  [where-clause]
  (let [[e a v] where-clause
        t (when (not= (last where-clause) v)
            (last where-clause))]
    {:e e :a a :v v :t t}))

(defn where-maps->var-map
  "Takes a list of where clauses in the form output from dissect-where
   and returns a map where the keys are the vars in the where clauses
   and the values are vectors of maps with the keys [:type :e :a :v :t].
   :type is where the var was in that where clause
   
   Example:
   (where-maps->var-map '[{:e ?e :a :bookmarks/type :v ?type :t nil}
                          {:e ?e :a :bookmarks/title :v ?title :t nil}])
   {?type [{:type :v
            :e ?e
            :a :bookmarks/type
            :v ?type
            :t nil}]
    ?title [{:type :v
             :e ?e
             :a :bookmarks/title
             :v ?title
             :t nil}]
   ?e [{:type :e
        :e ?e
        :a :bookmarks/type
        :v ?type
        :t nil}
       {:type :e
        :e ?e
        :a :bookmarks/title
        :v ?title
        :t nil}]}"
  [wheres]
  (->> wheres
       (mapcat (fn where->var-map-pairs
                 [where]
                 (->> where
                      (filter (comp symbol? second))
                      (map (fn type-var->var-map-pair 
                             [[where-type dvar]]
                             [dvar (merge where {:type where-type})])))))
       (group-by first)
       (map-vals (partial map second))))

(defn q-parts->resolved-vars
  "Takes the parts as output from q->parts
   and returns a map where the key is the var and the value is a map
   describing how the var is used, where it is used and how. The map
   has keys [:find :in :where]. 
   :find and :in are boolean, whether the var is used in :find or :in respectively
   :where is a vector of maps where the map has four keys, [:type :e :a :v],
      :type is which position the var is in for the clause (:e, :a or :v)
      :e is the value of the ent-id in the clause
      :a is the value of the attribute in the clause
      :v is the value of the value in the clause
   
   Example:
   (q-parts->resolved-vars '{:find (?title)
                            :in ($ ?type)
                            :where ([?e :bookmarks/type ?type] 
                                   [?e :bookmarks/title ?title])})

    {?title {:find true
             :in false
             :field :title
             :where [{:type :v
                      :e ?e
                      :a :bookmarks/title
                      :v ?title
                      :t nil}]}
    ?type {:find false
           :in true
           :in-index 1
           :field :type
           :where [{:type :v
                    :e ?e
                    :a :bookmarks/type
                    :v ?type
                    :t nil}]} 
    $ {:find false :field nil :in true :where nil :in-index 0}
    ?e {:find false
        :in false
        :field :id
        :where [{:type :e
                 :e ?e
                 :a :bookmarks/type
                 :v ?type
                 :t nil}
                {:type :e
                 :e ?e
                 :a :bookmarks/title
                 :v ?title
                 :t nil}]}}
   "
  [q-parts]
  (let [wheres (map dissect-where (:where q-parts))
        where-vars (filter symbol? (mapcat vals wheres))
        all-vars (concat
                   (:find q-parts)
                   (:in q-parts)
                   where-vars)
        vars (distinct all-vars)
        find-set (set (:find q-parts))
        in-map (fn [dvar]
                 (let [index (.indexOf (:in q-parts) dvar)]
                   (if (>= index 0)
                     {:in true
                      :in-index index}
                     {:in false})))
        field (fn [wheres]
                (let [v-where (first (filter #(= (:type %) :v) wheres))
                      e-where (first (filter #(= (:type %) :e) wheres))]
                  (cond
                    v-where (:field (datomic-attribute->table-field (:a v-where)))
                    e-where :id 
                    :else nil)))
        var->wheres (where-maps->var-map wheres)]
    (->> vars
         (mapv #(vector % 
                        (merge 
                          {:find (contains? find-set %)
                           :where (var->wheres %)
                           :field (field (var->wheres %))}
                          (in-map %))))
         (into {}))))

(defn should-ignore?
  "Returns true if var is a database symbol or _
   
   Examples:
   (should-ignore? '$)
   true
   (should-ignore? '_)
   true
   (should-ignore? '?test)
   false"
  [dvar]
  (or (clojure.string/starts-with? (str dvar) "_")
      (clojure.string/starts-with? (str dvar) "$")))

(defn build-sql-where
  "Given the var info and the list of inputs, 
   returns the correct sql where clause

   var-info can't have :find true

   Example:
   (build-sql-where '{:find false
                      :in true
                      :in-index 1
                      :field :type
                      :where [{:type :v
                               :e ?e
                               :a :bookmarks/type
                               :v ?type}]}
                        [db-conn \"web\"])
   {:type 'web'}"
  [var-info ins]
  (when (:find var-info)
    (throw (ex-info "Don't create sql where for find elements" 
                    {:var-info var-info})))
  (when-not (:in var-info)
    (throw (ex-info "Can't create sql where for non in vars currently" 
                    {:var-info var-info})))
  ;; TODO: Support more general queries 
  (let [where (first (filter #(= (:type %) :v) (:where var-info))) ]  
    (if (< (:in-index var-info) (count ins))
      {(:field var-info) (nth ins (:in-index var-info))}
      (throw (ex-info "Not enouch inputs for query" {:var-info var-info :ins ins})))))

;;TODO: fix limitations
;;  * No support for ref
;;  * No support for multiple 'tables'
;;  * Requires that an attribute is explicitly used in where
(defn build-sql 
  "Turns a datomic query into sql
   
   Example:
   (build-sql '{:find (?title)
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
           [db \"web\"])
   {:table :bookmarks
    :select [:title]
    :where {:type 'web'}}"
  [q-parts vars ins]
  (let [
        example-attribute (->> (:where q-parts)
                               (map dissect-where)
                               (remove (comp symbol? :a))
                               first)
        {:keys [table]} (datomic-attribute->table-field (:a example-attribute))
        selector-vars (filter (comp :find second) vars)
        selectors (mapv :field (vals selector-vars))
        where-vars (remove (fn [[dvar info]] 
                             (or (:find info)
                                 (should-ignore? dvar)))
                           vars)
        wheres (reduce merge (map #(build-sql-where % ins) (vals where-vars)))]
    {:table table
     :select selectors
     :where wheres}))

(defn q
  "Meant to look like datomic.api/q
   
   Given input like datomic.api/q, switching the datomic db
   for a sql db spec, uses the datomic spec to build a sql query 
   and execute the query. Returns the output in the same form datomic would
   
   Example:

    (q '[:find ?title
         :in $ ?type
         :where 
         [_ :bookmarks/type ?type]
         [_ :bookmarks/title ?title]]
       {:classname \"org.sqlite.JDBC\"
        :subprotocol \"sqlite\"
        :subname \"test.db\"}
       \"web\")
   #{[\"my-cool-title\"] [\"my-cool-title2\"]}"
  [query & ins]
  (let [db (first ins)
        q-parts (q->parts query)
        vars (q-parts->resolved-vars q-parts) 
        sql (build-sql q-parts vars ins)
        finders (->> vars
                     (filter (comp :find second))
                     vec
                     (into {}))
        finders-ordered (map finders (:find q-parts))
        finder-fields (map :field finders-ordered)
        out (oj/exec sql db)]
    (set (map (apply juxt finder-fields) out))))
