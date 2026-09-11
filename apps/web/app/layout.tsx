import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Worth the drive? | Grocery savings",
  description: "Compare grocery deal savings with the fuel cost of a dedicated trip.",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
