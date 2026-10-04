CREATE TABLE "relay_hook_mailbox" (
	"id" varchar(36) PRIMARY KEY,
	"environment_id" varchar(191) NOT NULL,
	"received_at" varchar(64) NOT NULL,
	"expires_at" varchar(64) NOT NULL,
	"method" varchar(16) NOT NULL,
	"raw_hook_id" varchar(512) NOT NULL,
	"raw_token" varchar(512) NOT NULL,
	"query" text NOT NULL,
	"headers" jsonb NOT NULL,
	"body" bytea NOT NULL
);
--> statement-breakpoint
ALTER TABLE "relay_environment_links" ADD COLUMN "hold_webhooks_while_offline" boolean DEFAULT false NOT NULL;--> statement-breakpoint
CREATE INDEX "idx_relay_hook_mailbox_environment" ON "relay_hook_mailbox" ("environment_id","received_at");--> statement-breakpoint
CREATE INDEX "idx_relay_hook_mailbox_expires" ON "relay_hook_mailbox" ("expires_at");