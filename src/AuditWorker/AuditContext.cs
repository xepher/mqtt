using System;
using Microsoft.EntityFrameworkCore;

namespace AuditWorker
{
    public class AuditLog
    {
        public int Id { get; set; }
        public string DeviceId { get; set; }
        public string User { get; set; }
        public string Action { get; set; }
        public string Result { get; set; }
        public DateTime Timestamp { get; set; }
        public string WorkerName { get; set; }
    }

    public class AuditContext : DbContext
    {
        public DbSet<AuditLog> AuditLogs { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            // SQLite connection string with Cache=Shared and Busy Timeout to handle concurrent write locks
            optionsBuilder.UseSqlite("Data Source=data/audit.db;Cache=Shared;Mode=ReadWriteCreate;Default Timeout=5");
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<AuditLog>().HasKey(x => x.Id);
            modelBuilder.Entity<AuditLog>().Property(x => x.DeviceId).IsRequired(false);
            modelBuilder.Entity<AuditLog>().Property(x => x.User).IsRequired(false);
            modelBuilder.Entity<AuditLog>().Property(x => x.Action).IsRequired(false);
            modelBuilder.Entity<AuditLog>().Property(x => x.Result).IsRequired(false);
            modelBuilder.Entity<AuditLog>().Property(x => x.WorkerName).IsRequired(false);
        }
    }
}
