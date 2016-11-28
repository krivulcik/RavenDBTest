using MoreLinq;
using NLog;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;

namespace RavenDBTest
{
    class Program
    {
        private static Logger _logger = LogManager.GetLogger("RavenDBTest");
        private static Random _rng = new Random();

        public class Employee
        {
            public string Id { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }
            public DateTime LastDate { get; set; }
        }

        public class EmployeeParameters
        {
            public string Id { get; set; }
            public string EmployeeId { get; set; }
            public string Value1 { get; set; }
            public string Value2 { get; set; }
            public int[] Values { get; set; }
        }

        public class Contract
        {
            public string Id { get; set; }
            public string Text { get; set; }
            public string AccessKey { get; set; }
            public string PlaceholderText { get; set; }
            public double?[] PlaceholderValues { get; set; }
        }

        public class Assignment
        {
            public string Id { get; set; }
            public string ContractId { get; set; }
            public string[] Employees { get; set; }
        }

        public class Parameters
        {
            public string Id { get; set; }
            public string ContractId { get; set; }
            public string Revision { get; set; }
            public int Value1 { get; set; }
            public int Value2 { get; set; }
            public int Value3 { get; set; }
            public int Value4 { get; set; }
            public double Value5 { get; set; }
            public double Value6 { get; set; }
            public double Value7 { get; set; }
            public double Value8 { get; set; }
            public double?[] PlaceholderValues { get; set; }
        }

        public class EmployeeView
        {
            public string Id { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }
            public string Name { get; set; }
            public DateTime LastDate { get; set; }
            public string Value1 { get; set; }
        }

        public class ContractFull
        {
            public string ContractId { get; set; }
            public string Text { get; set; }
            public string AccessKey { get; set; }
            public string Revision { get; set; }
            public int? Value1 { get; set; }
            public int? Value2 { get; set; }
            public int? Value3 { get; set; }
            public int? Value4 { get; set; }
            public double? Value5 { get; set; }
            public double? Value6 { get; set; }
            public double? Value7 { get; set; }
            public double? Value8 { get; set; }
            public string[] Employees { get; set; }
        }

        public class EmployeeIndex : AbstractIndexCreationTask<Employee, EmployeeView>
        {
            public EmployeeIndex()
            {
                Map = employees => from employee in employees
                                   let parameters = LoadDocument<EmployeeParameters>(employee.Id.Replace("e-", "ep-"))
                                   select new
                                   {
                                       employee.Id,
                                       employee.FirstName,
                                       employee.LastName,
                                       Name = string.Join(" ", new[] { employee.FirstName, employee.LastName }.Where(x => !string.IsNullOrEmpty(x))),
                                       LastDate = employee.LastDate,
                                       Value1 = parameters.Value1,
                                   };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        public class ContractFullIndex : AbstractMultiMapIndexCreationTask<ContractFull>
        {
            public ContractFullIndex()
            {
                AddMap<Contract>(
                    contracts => from contract in contracts
                                 let assignment = LoadDocument<Assignment>("a-" + contract.Id)
                                 select new
                                 {
                                     ContractId = contract.Id,
                                     contract.Text,
                                     contract.AccessKey,
                                     Revision = (int?)null,
                                     Value1 = (int?)null,
                                     Value2 = (int?)null,
                                     Value3 = (int?)null,
                                     Value4 = (int?)null,
                                     Value5 = (double?)null,
                                     Value6 = (double?)null,
                                     Value7 = (double?)null,
                                     Value8 = (double?)null,
                                     Employees = assignment != null ? assignment.Employees : new string[] { },
                                 });
                AddMap<Parameters>(
                    parameters => from parameter in parameters
                                  select new
                                  {
                                      ContractId = parameter.ContractId,
                                      Text = (string)null,
                                      AccessKey = (string)null,
                                      parameter.Revision,
                                      parameter.Value1,
                                      parameter.Value2,
                                      parameter.Value3,
                                      parameter.Value4,
                                      parameter.Value5,
                                      parameter.Value6,
                                      parameter.Value7,
                                      parameter.Value8,
                                      Employees = new string[] { },
                                  });

                Reduce = results => from result in results
                                    group result by result.ContractId
                                    into g
                                    let contract = g.FirstOrDefault(x => !string.IsNullOrEmpty(x.AccessKey))
                                    let lastValue = g.Where(x => x.Revision != null).OrderByDescending(x => x.Revision).FirstOrDefault()
                                    select new
                                    {
                                        ContractId = g.Key,
                                        Text = contract != null ? contract.Text : null,
                                        AccessKey = contract != null ? contract.AccessKey : null,
                                        Revision = lastValue != null ? lastValue.Revision : null,
                                        Value1 = lastValue != null ? lastValue.Value1 : null,
                                        Value2 = lastValue != null ? lastValue.Value2 : null,
                                        Value3 = lastValue != null ? lastValue.Value3 : null,
                                        Value4 = lastValue != null ? lastValue.Value4 : null,
                                        Value5 = lastValue != null ? lastValue.Value5 : null,
                                        Value6 = lastValue != null ? lastValue.Value6 : null,
                                        Value7 = lastValue != null ? lastValue.Value7 : null,
                                        Value8 = lastValue != null ? lastValue.Value8 : null,
                                        Employees = contract.Employees != null && contract.Employees.Length > 0 ? contract.Employees : (string[])null,
                                    };

                Suggestion(x => x.Text);

                Sort(x => x.Value1, SortOptions.Int);
                Sort(x => x.Value2, SortOptions.Int);
                Sort(x => x.Value3, SortOptions.Int);
                Sort(x => x.Value4, SortOptions.Int);
                Sort(x => x.Value5, SortOptions.Double);
                Sort(x => x.Value6, SortOptions.Double);
                Sort(x => x.Value7, SortOptions.Double);
                Sort(x => x.Value8, SortOptions.Double);

                Indexes.Add(x => x.Text, FieldIndexing.Analyzed);

                StoreAllFields(FieldStorage.Yes);
            }
        }



        public static void Main()
        {
            bool initializeDb = true;
            var contracts = 100000;
            var employees = 2000;
            var assignments = 30000;
            using (IDocumentStore store = new DocumentStore { Url = "http://localhost:8080/", DefaultDatabase = "indextest" })
            {
                store.Initialize();

                if (initializeDb)
                {
                    _logger.Info("Generating indexes.");
                    Console.WriteLine("About to create indexes. Confirm by Enter.");
                    Console.ReadLine();
                    new EmployeeIndex().Execute(store);
                    new ContractFullIndex().Execute(store);

                    _logger.Info("Indexes generated, creating documents.");
                    Console.WriteLine("About to create documents. Confirm by Enter.");
                    Console.ReadLine();
                    GenerateData(store, employees, contracts, assignments);

                    _logger.Info("Documents created.");

                    bool stale = true;
                    do
                    {
                        try
                        {
                            using (var session = store.OpenSession())
                            {
                                session.Query<ContractFull, ContractFullIndex>()
                                    .Customize(x => x.WaitForNonStaleResults())
                                    .Count();
                                stale = false;
                            }
                        }
                        catch (TimeoutException)
                        {
                            _logger.Info("Timeout exception while waiting for index recomputation. Sleeping for 30 seconds.");
                            Thread.Sleep(30000);
                        }
                    }
                    while (stale);

                    _logger.Info("Database prepared.");
                }

                ConcurrentQueue<string> queue = new ConcurrentQueue<string>();

                {
                    long changeno = 0;
                    int changethreads = 0;
                    for (int i = 0; i < 1; i++)
                    {
                        _logger.Debug("Starting change thread.");
                        var thread = new Thread(() =>
                            {
                                while (true)
                                {
                                    Interlocked.Increment(ref changeno);
                                    if (changeno % 1000 == 0)
                                    {
                                        _logger.Debug("Changes: " + changeno);
                                    }
                                    try
                                    {
                                        if (queue.Count < 100)
                                        {
                                            Thread.Sleep(1000);
                                            continue;
                                        }

                                        var ids = queue.Take(100).Distinct().ToArray();
                                        queue = new ConcurrentQueue<string>();

                                        using (var session = store.OpenSession())
                                        using (var transaction = new TransactionScope())
                                        {
                                            var assignmentDocs = session.Load<Assignment>(ids.Select(x => "a-" + x)).Where(x => x != null);

                                            foreach (var assignment in assignmentDocs)
                                            {
                                                var oldEmployees = assignment.Employees;
                                                assignment.Employees = assignment.Employees.Skip(1).Concat(new[] { _rng.Next(1, employees + 1).ToString().PadLeft(10, '0') }).Distinct().ToArray();

                                                //_logger.Trace($"Changed {assignment.Id}: old: {string.Join(",", oldEmployees)}, new: {string.Join(",", assignment.Employees)}");
                                                session.Store(assignment);
                                            }

                                            session.SaveChanges();
                                            transaction.Complete();
                                        }

                                        using (var session = store.OpenSession())
                                        using (var transaction = new TransactionScope())
                                        {
                                            var items = session.Query<ContractFull, ContractFullIndex>()
                                                .Customize(x => x.WaitForNonStaleResultsAsOfLastWrite())
                                                .Where(x => x.ContractId.In(ids))
                                                .ToArray();

                                            if (items.Length != ids.Length)
                                            {
                                                throw new Exception("Mismatch in ContractIndex found - incorrect count.");
                                            }
                                            foreach (var item in items)
                                            {
                                                if (string.IsNullOrEmpty(item.AccessKey)
                                                    || string.IsNullOrEmpty(item.Text)
                                                    || item.Value1 == null)
                                                {
                                                    throw new Exception("Mismatch in ContractIndex found.");
                                                }
                                            }
                                        }
                                    }
                                    catch (ConcurrencyException)
                                    {
                                        _logger.Trace("ConcurrencyException ignored.");
                                    }
                                    catch (TransactionAbortedException)
                                    {
                                        _logger.Trace("TransactionAbortedException ignored.");
                                    }
                                }
                            });
                        changethreads++;
                        thread.Start();
                    }
                }

                {
                    long readno = 0;
                    var readthreads = 0;
                    for (int i = 0; i < 10; i++)
                    {
                        _logger.Debug("Starting read thread.");
                        var thread = new Thread(() =>
                        {
                            while (true)
                            {
                                Interlocked.Increment(ref readno);
                                if (readno % 1000 == 0)
                                {
                                    _logger.Debug("Reads: " + readno);
                                }
                                using (var session = store.OpenSession())
                                {
                                    var employeeId = _rng.Next(1, employees + 1);
                                    var employeeStrId = "e-" + employeeId.ToString().PadLeft(10, '0');
                                    var employee = session.Load<Employee>(employeeStrId);

                                    var employeeView = session.Query<EmployeeView, EmployeeIndex>().Where(x => x.Id == employeeStrId)
                                        .ProjectFromIndexFieldsInto<EmployeeView>()
                                        .First();
                                    if (string.IsNullOrEmpty(employeeView.FirstName)
                                        || string.IsNullOrEmpty(employeeView.Value1)
                                        || employeeView.LastDate == null)
                                    {
                                        Console.WriteLine(employeeView.FirstName);
                                        Console.WriteLine(employeeView.Value1);
                                        Console.WriteLine(employeeView.LastDate.ToShortDateString());

                                        throw new Exception("Mismatch in EmployeeIndex found.");
                                    }
                                    session.SaveChanges();
                                }
                                Thread.Sleep(_rng.Next(10, 101));
                            }
                        });
                        readthreads++;
                        thread.Start();
                    }
                }

                {
                    long dreadno = 0;
                    var dreadthreads = 0;
                    for (int i = 0; i < 50; i++)
                    {
                        _logger.Debug("Starting document read thread.");
                        var thread = new Thread(() =>
                        {
                            while (true)
                            {
                                Interlocked.Increment(ref dreadno);
                                if (dreadno % 1000 == 0)
                                {
                                    _logger.Debug("DReads: " + dreadno);
                                }
                                using (var session = store.OpenSession())
                                {
                                    var employeeId = _rng.Next(1, employees + 1);
                                    var employeeStrId = "e-" + employeeId.ToString().PadLeft(10, '0');
                                    var employee = session.Load<Employee>(employeeStrId);
                                    employee.LastDate = DateTime.Now;
                                    session.Store(employee);

                                    var id = _rng.Next(1, contracts + 1);
                                    var strid = id.ToString().PadLeft(10, '0');
                                    var contract = session.Load<Contract>(strid);
                                    if (contract == null)
                                    {
                                        continue;
                                    }
                                    queue.Enqueue(strid);
                                    var contractFull = session.Query<ContractFull, ContractFullIndex>().Where(x => x.ContractId == strid)
                                        .ProjectFromIndexFieldsInto<ContractFull>()
                                        .First();
                                    if (string.IsNullOrEmpty(contractFull.AccessKey)
                                        || string.IsNullOrEmpty(contractFull.Text)
                                        || contractFull.Value1 == null)
                                    {
                                        throw new Exception("Mismatch in ContractIndex found.");
                                    }
                                    session.SaveChanges();
                                }
                                Thread.Sleep(_rng.Next(10, 101));
                            }
                        });
                        dreadthreads++;
                        thread.Start();
                    }
                }

                while (true)
                {
                    Thread.Sleep(10000);
                }
            }
        }

        private static void GenerateData(IDocumentStore store, int employees, int contracts, int assignments)
        {
            var batchSize = 512;
            var employeeData = Enumerable.Range(1, employees).Select(x => x.ToString().PadLeft(10, '0'))
                .Select(x => new {
                    Employee = new Employee
                    {
                        Id = "e-" + x,
                        FirstName = x,
                        LastName = x,
                        LastDate = DateTime.Now,
                    },
                    EmployeeParameters = new EmployeeParameters
                    {
                        Id = "ep-" + x,
                        EmployeeId = "e-" + x,
                        Value1 = RandomString(10),
                        Value2 = RandomString(500),
                        Values = Enumerable.Range(1, 100).Select(_ => _rng.Next()).ToArray(),
                    }
                });

            var batchNo = 0;
            var employeeBatches = (int)Math.Ceiling(employees / (double)batchSize);
            foreach (var batch in employeeData.Batch(batchSize))
            {
                using (var session = store.OpenSession())
                {
                    foreach (var employee in batch)
                    {
                        session.Store(employee.Employee);
                        session.Store(employee.EmployeeParameters);
                    }
                    ++batchNo;
                    _logger.Info($"Saving employees batch {batchNo} / {employeeBatches}");
                    session.SaveChanges();
                }
            }

            var contractData = Enumerable.Range(1, contracts).Select(x => x.ToString().PadLeft(10, '0'))
                .Select(idstring => new
                {
                    Contract = new Contract
                    {
                        Id = idstring,
                        Text = string.Join(" ", Enumerable.Range(1, 20).Select(x => RandomString(8))),
                        AccessKey = string.Join("-", Enumerable.Repeat(idstring, 5)),
                        PlaceholderText = string.Join(" ", Enumerable.Range(1, 300).Select(x => RandomString(10))),
                        PlaceholderValues = Enumerable.Range(1, 500).Select(_ => (double?)_rng.NextDouble()).ToArray(),
                    },
                    Revisions = Enumerable.Range(1, 5).Select(x => x.ToString().PadLeft(2, '0'))
                        .Select(revision => new Parameters
                        {
                            Id = idstring + "|" + revision,
                            ContractId = idstring,
                            Revision = revision,
                            Value1 = _rng.Next(),
                            Value2 = _rng.Next(),
                            Value3 = _rng.Next(),
                            Value4 = _rng.Next(),
                            Value5 = _rng.NextDouble(),
                            Value6 = _rng.NextDouble(),
                            Value7 = _rng.NextDouble(),
                            Value8 = _rng.NextDouble(),
                            PlaceholderValues = Enumerable.Range(1, 500).Select(_ => (double?)_rng.NextDouble()).ToArray(),
                        })
                });

            batchNo = 0;
            var contractBatches = (int)Math.Ceiling(contracts / (double)batchSize);
            foreach (var batch in contractData.Batch(batchSize))
            {
                using (var session = store.OpenSession())
                {
                    foreach (var contract in batch)
                    {
                        session.Store(contract.Contract);
                        foreach (var parameters in contract.Revisions)
                        {
                            session.Store(parameters);
                        }
                    }
                    ++batchNo;
                    _logger.Info($"Saving contracts batch {batchNo} / {contractBatches}.");
                    session.SaveChanges();
                }
            }

            var assignmentsData = Enumerable.Range(1, contracts).OrderBy(_ => _rng.NextDouble()).Take(assignments).Select(x => x.ToString().PadLeft(10, '0'))
                .Select(contractId => new Assignment
                {
                    Id = "a-" + contractId,
                    ContractId = contractId,
                    Employees = Enumerable.Range(1, _rng.Next(1, 11)).Select(_ => _rng.Next(1, employees + 1).ToString().PadLeft(10, '0')).Distinct().ToArray(),
                });

            batchNo = 0;
            var assignmentBatches = (int)Math.Ceiling(assignments / (double)batchSize);
            foreach (var batch in assignmentsData.Batch(batchSize))
            {
                using (var session = store.OpenSession())
                {
                    foreach (var assignment in batch)
                    {
                        session.Store(assignment);
                    }
                    ++batchNo;
                    _logger.Info($"Saving assignments batch {batchNo} / {assignmentBatches}.");
                    session.SaveChanges();
                }
            }
        }

        // http://stackoverflow.com/a/8996788/4066170
        public static string RandomString(int length, string allowedChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
        {
            if (length < 0) throw new ArgumentOutOfRangeException("length", "length cannot be less than zero.");
            if (string.IsNullOrEmpty(allowedChars)) throw new ArgumentException("allowedChars may not be empty.");

            const int byteSize = 0x100;
            var allowedCharSet = new HashSet<char>(allowedChars).ToArray();
            if (byteSize < allowedCharSet.Length) throw new ArgumentException(string.Format("allowedChars may contain no more than {0} characters.", byteSize));

            var result = new StringBuilder();
            var buf = new byte[128];
            while (result.Length < length) {
                _rng.NextBytes(buf);
                for (var i = 0; i < buf.Length && result.Length < length; ++i) {
                    // Divide the byte into allowedCharSet-sized groups. If the
                    // random value falls into the last group and the last group is
                    // too small to choose from the entire allowedCharSet, ignore
                    // the value in order to avoid biasing the result.
                    var outOfRangeStart = byteSize - (byteSize % allowedCharSet.Length);
                    if (outOfRangeStart <= buf[i]) continue;
                    result.Append(allowedCharSet[buf[i] % allowedCharSet.Length]);
                }
            }
            return result.ToString();
        }
    }
}
